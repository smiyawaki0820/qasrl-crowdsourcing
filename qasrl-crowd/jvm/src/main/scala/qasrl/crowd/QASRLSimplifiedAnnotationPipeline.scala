package qasrl.crowd

import akka.actor._
import cats.implicits._
import com.amazonaws.services.mturk.model._
import com.typesafe.scalalogging.StrictLogging
import nlpdata.datasets.wiktionary.Inflections
import nlpdata.structure._
import nlpdata.util.HasTokens.ops._
import nlpdata.util.LowerCaseStrings._
import nlpdata.util.{HasTokens, PosTags, Text}
import qasrl.crowd.util.PosTagger
import qasrl.crowd.util.implicits._
import spacro._
import spacro.tasks._
import spacro.util.Span
import upickle.default._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random


class QASRLSimplifiedAnnotationPipeline[SID : Reader : Writer : HasTokens](
  val allIds: Vector[SID], // IDs of sentences to annotate
  numGenerationAssignmentsForPrompt: Int,
  numActivePrompts: Int,
  phase: Phase,
  annotationDataService: AnnotationDataService,
  frozenGenerationHITTypeId: Option[String] = None)(
  implicit val config: TaskConfig,
  val settings: QASRLSettings,
  val inflections: Inflections
) extends StrictLogging {

  def getKeyIndices(id: SID): Set[Int] = {
    val posTaggedTokens = PosTagger.posTag(id.tokens)
    posTaggedTokens.collect {
      case Word(index, pos, token) if PosTags.verbPosTags.contains(pos) =>
        if( // detect if "have"-verb is an auxiliary
          Inflections.haveVerbs.contains(token.lowerCase) &&
            (posTaggedTokens.lift(index + 1).map(_.token.lowerCase).nonEmptyAnd(Inflections.negationWords.contains) || // negation appears directly after, or
               posTaggedTokens.drop(index + 1).forall(_.pos != "VBN") || // there is no past-participle verb afterward, or
               posTaggedTokens.drop(index + 1) // after the "have" verb,
               .takeWhile(_.pos != "VBN") // until the next past-participle form verb,
               .forall(w => Inflections.negationWords.contains(w.token.lowerCase) || PosTags.adverbPosTags.contains(w.pos)) // everything is an adverb or negation (though I guess negs are RB)
            )
        ) None else if( // detect if "do"-verb is an auxiliary
          Inflections.doVerbs.contains(token.lowerCase) &&
            (posTaggedTokens.lift(index + 1).map(_.token.lowerCase).nonEmptyAnd(Inflections.negationWords.contains) || // negation appears directly after, or
               posTaggedTokens.drop(index + 1).forall(w => w.pos != "VB" && w.pos != "VBP") || // there is no stem or non-3rd-person present verb afterward (to mitigate pos tagger mistakes), or
               posTaggedTokens.drop(index + 1) // after the "do" verb,
               .takeWhile(w => w.pos != "VBP" && w.pos != "VBP") // until the next VB or VBP verb,
               .forall(w => Inflections.negationWords.contains(w.token.lowerCase) || PosTags.adverbPosTags.contains(w.pos)) // everything is an adverb or negation (though I guess negs are RB)
            )
        ) None else inflections.getInflectedForms(token.lowerCase).map(_ => index)
    }.flatten.toSet
  }

  lazy val allPrompts: Vector[QASRLGenerationPrompt[SID]] = Random.shuffle(for {
    id <- allIds
    verbIndex <- getKeyIndices(id).toList.sorted
  } yield QASRLGenerationPrompt(id, verbIndex))

  lazy val assignLimit: Int = allPrompts.length
  logger.info(s"Assignment Limit: $assignLimit")

  implicit val ads = annotationDataService

  import config.hitDataService


  private def createQualification(name: String, description: String):QualificationType= {
    val qualResult = config.service.createQualificationType(
      new CreateQualificationTypeRequest()
        .withName(name)
        .withKeywords(KEYWORDS)
        .withDescription(description)
        .withQualificationTypeStatus(QualificationTypeStatus.Active)
    )
    qualResult.getQualificationType
  }

  private def findQualificationType (qualificationName: String): Option[QualificationType] = {
    val qualificationTypes = config.service.listQualificationTypes(
      new ListQualificationTypesRequest()
        .withQuery(qualificationName)
        .withMustBeOwnedByCaller(true)
        .withMustBeRequestable(false)
        .withMaxResults(100)
    ).getQualificationTypes.asScala.toList
    val found = qualificationTypes.find(_.getName == qualificationName)
    found
  }

  private def createQualificationReq(qualification: QualificationType, shouldHave: Boolean): QualificationRequirement = {
    val comparator = if (shouldHave) "Exists" else "DoesNotExist"
    val req = new QualificationRequirement()
      .withQualificationTypeId(qualification.getQualificationTypeId)
      .withComparator(comparator)
      .withRequiredToPreview(false)
    req
  }

  private val KEYWORDS = "language,english,question answering"

  val approvalRateQualificationTypeID = "000000000000000000L0"
  val approvalRateRequirement = new QualificationRequirement()
    .withQualificationTypeId(approvalRateQualificationTypeID)
    .withComparator("GreaterThanOrEqualTo")
    .withIntegerValues(95)
    .withRequiredToPreview(false)

  val localeQualificationTypeID = "00000000000000000071"
  val localeRequirement = new QualificationRequirement()
    .withQualificationTypeId(localeQualificationTypeID)
    .withComparator("NotEqualTo")
    .withLocaleValues(new Locale().withCountry("IN"))
    .withRequiredToPreview(false)


  val genCoverageDisqualTypeName = "Questions asked per verb disqualification"

  val genCoverageDisqualType = findQualificationType(genCoverageDisqualTypeName).getOrElse{
    logger.info("Generating generation coverage disqualification type...")
    createQualification(genCoverageDisqualTypeName, "Number of questions asked for each verb in our question-answer " +
      "pair generation task is too low.")
  }
  val genCoverageRequirement = createQualificationReq(genCoverageDisqualType, false)

  val genAccDisqualTypeName = "Question-answer writing accuracy disqualification"
  val genAccDisqualType = findQualificationType(genAccDisqualTypeName).getOrElse {
    logger.info("Generating generation accuracy disqualification type...")
    createQualification(genAccDisqualTypeName, "Accuracy on the question-answer writing task is too low.")
  }

  val genAccuracyRequirement = createQualificationReq(genAccDisqualType, shouldHave = false)

  val genTrainingQualName = "Training and Qualification Phase for annotators in question generation"
  val genProductionQualName = "Production Phase for annotators in question generation"

  val genTrainingQualType = findQualificationType(genTrainingQualName).getOrElse{
    logger.info("Generating generation training qualification type...")
    createQualification(genTrainingQualName,
      description="""Access granted to the training and qualification rounds
        |in writing questions and answers""".stripMargin)
  }

  val genProductionQualType = findQualificationType(genProductionQualName).getOrElse{
    logger.info("Generating generation production qualification type...")
    createQualification(genProductionQualName,
      description="""Access granted to the live annotation round
          |in writing questions and answers""".stripMargin)
  }

  val InTrainingReq = createQualificationReq(genTrainingQualType, shouldHave = true)
  val NotInTrainingReq = createQualificationReq(genTrainingQualType, shouldHave = false)
  val InProductionReq = createQualificationReq(genProductionQualType, shouldHave = true)
  val NotInProductionReq = createQualificationReq(genProductionQualType, shouldHave = false)


  def revokeAllWorkerQuals(qualTypeId: String, reason: String = "") = {
    val quals = config.service.listWorkersWithQualificationType(
      new ListWorkersWithQualificationTypeRequest()
        .withQualificationTypeId(qualTypeId)
        .withMaxResults(100)
    ).getQualifications.asScala.toList
    for (qual <- quals) {
      logger.info(s"Restoring qualificiation: $qualTypeId for worker: ${qual.getWorkerId}")
      config.service.disassociateQualificationFromWorker(
        new DisassociateQualificationFromWorkerRequest()
          .withQualificationTypeId(qualTypeId)
          .withWorkerId(qual.getWorkerId)
      )
    }
  }

  // NOTE may need to call multiple times to cover all workers... sigh TODO pagination
  def resetAllQualificationValues = {
    revokeAllWorkerQuals(genCoverageDisqualType.getQualificationTypeId)
    revokeAllWorkerQuals(genAccDisqualType.getQualificationTypeId)
    revokeAllWorkerQuals(genTrainingQualType.getQualificationTypeId)
    revokeAllWorkerQuals(genProductionQualType.getQualificationTypeId)
  }

  lazy val (taskPageHeadLinks, taskPageBodyLinks) = {
    import scalatags.Text.all._
    val headLinks = List(
      link(
        rel := "stylesheet",
        href := "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.6/css/bootstrap.min.css",
        attr("integrity") := "sha384-rwoIResjU2yc3z8GV/NPeZWAv56rSmLldC3R/AZzGRnGxQQKnKkoFVhFQhNUwEyJ",
        attr("crossorigin") := "anonymous"))
    val bodyLinks = List(
      script(
        src := "https://code.jquery.com/jquery-3.1.1.slim.min.js",
        attr("integrity") := "sha384-A7FZj7v+d/sdmMqp/nOQwliLvUsJfDHW+k9Omg/a/EheAdgtzNs3hpfag6Ed950n",
        attr("crossorigin") := "anonymous"),
      script(
        src := "https://cdnjs.cloudflare.com/ajax/libs/jquery-cookie/1.4.1/jquery.cookie.min.js",
        attr("integrity") := "sha256-1A78rJEdiWTzco6qdn3igTBv9VupN3Q1ozZNTR4WE/Y=",
        attr("crossorigin") := "anonymous"),
      script(
        src := "https://cdnjs.cloudflare.com/ajax/libs/tether/1.4.0/js/tether.min.js",
        attr("integrity") := "sha384-DztdAPBWPRXSA/3eYEEUWrWCy7G5KFbe8fFjk5JAIxUYHKkDx6Qin1DkWx51bBrb",
        attr("crossorigin") := "anonymous"),
      script(
        src := "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-alpha.6/js/bootstrap.min.js",
        attr("integrity") := "sha384-vBWWzlZJ8ea9aCX4pEW3rVHjgjt7zpkNpZk+02D9phzyeVkE+jo0ieGizqPLForn",
        attr("crossorigin") := "anonymous"))
    (headLinks, bodyLinks)
  }

  val genHITType = selectHitType(phase)

  private def selectHitType(phase: Phase) = {
    val titleSuffix = phase match {
      case Training => "[Qualification]"
      case Production => "[Production]"
      case default => ""
    }
    val descriptionPrefix = phase match {
      case Training => """[Training and qualification]
        |Your work will be reviewed by expert annotators. Read carefully the supplied instructions and act according to
        |the received feedback. Successful accomplishment of this training round is essential to
        |access more hits in the annotation rounds""".stripMargin
      case Production =>
        s"""[Production phase]
           |Your work will be sampled and reviewed by expert annotators. From time to time you will receive feedback
           |concerning your work. You are expected to read and act accordingly.
         """.stripMargin
      case default => ""
    }

    val phaseRequirements = phase match {
      case Training => List(InTrainingReq, NotInProductionReq)
      case Production => List(InProductionReq, NotInTrainingReq)
      case default => List(NotInProductionReq, NotInTrainingReq)
    }

    HITType(
      title = s"Write question-answer pairs about a verb ${titleSuffix}",
      description =s"""${descriptionPrefix}

      Given a sentence and a verb from that sentence,
      write questions and answers about that verb.
      Questions must adhere to a certain template,
      provided by autocomplete functionality.
      Workers who maintain high accuracy will be contacted
      for further large-scale annotation efforts.
    """.trim.replace("\\s+", " "),
      reward = settings.generationReward,
      keywords = "language,english,question answering",
      qualRequirements = Array[QualificationRequirement](
        approvalRateRequirement, localeRequirement, genCoverageRequirement, genAccuracyRequirement)
        ++ phaseRequirements,
      autoApprovalDelay = 2592000L, // 30 days
      assignmentDuration = 600L)
  }

  lazy val genAjaxService = new Service[QASRLGenerationAjaxRequest[SID]] {
    override def processRequest(request: QASRLGenerationAjaxRequest[SID]) = request match {
      case QASRLGenerationAjaxRequest(workerIdOpt, QASRLGenerationPrompt(id, verbIndex)) =>
        val questionListsOpt = for {
          genManagerP <- Option(genManagerPeek)
          workerId <- workerIdOpt
          qCounts <- genManagerP.coverageStats.get(workerId)
        } yield qCounts
        val questionLists = questionListsOpt.getOrElse(Nil)

        val workerStatsOpt = for {
          accTrackP <- Option(accuracyTrackerPeek)
          workerId <- workerIdOpt
          stats <- accTrackP.allWorkerStats.get(workerId)
        } yield stats

        val stats = GenerationStatSummary(
          numVerbsCompleted = questionLists.size,
          numQuestionsWritten = questionLists.sum,
          workerStatsOpt = workerStatsOpt)

        val tokens = id.tokens
        val inflectedForms = inflections.getInflectedForms(tokens(verbIndex).lowerCase).get
        QASRLGenerationAjaxResponse(stats, tokens, inflectedForms)
    }
  }

  // hit management --- circularly defined so they can communicate

  import config.actorSystem

  var accuracyTrackerPeek: QASRLGenerationAccuracyManager[SID] = null

  lazy val accuracyTracker: ActorRef = actorSystem.actorOf(
    Props {
      accuracyTrackerPeek = new QASRLGenerationAccuracyManager[SID]("")
      accuracyTrackerPeek
    }
  )

  val genTaskKey = phase match {
    case Trap => settings.generationTrapTaskKey
    case Training => settings.generationTrainTaskKey
    case Production => settings.generationProdTaskKey
  }

  val genTaskSpec = TaskSpecification.NoWebsockets[QASRLGenerationPrompt[SID], List[VerbQA], QASRLGenerationAjaxRequest[SID]](
    genTaskKey, genHITType, genAjaxService, allPrompts,
    taskPageHeadElements = taskPageHeadLinks,
    taskPageBodyElements = taskPageBodyLinks,
    frozenHITTypeId = frozenGenerationHITTypeId)

  var genManagerPeek: QASRLGenerationSimplifiedHITManager[SID] = null

  val genHelper = new HITManager.Helper(genTaskSpec)
  val genManager: ActorRef = actorSystem.actorOf(
    Props {
      genManagerPeek = new QASRLGenerationSimplifiedHITManager(
        genHelper,
        genCoverageDisqualType.getQualificationTypeId,
        assignLimit,
        if(config.isProduction) _ => numGenerationAssignmentsForPrompt else _ => 1,
        if(config.isProduction) numActivePrompts else 3,
        allPrompts.iterator)
      genManagerPeek
    }
  )
  val genActor = actorSystem.actorOf(Props(new TaskManager(genHelper, genManager)))

  lazy val server = new Server(List(genTaskSpec))

  // used to schedule data-saves
  private[this] var schedule: List[Cancellable] = Nil
  def startSaves(interval: FiniteDuration = 5 minutes): Unit = {
    if(schedule.exists(_.isCancelled) || schedule.isEmpty) {
      schedule = List(genManager).map(actor =>
        config.actorSystem.scheduler.schedule(
          2 seconds, interval, actor, SaveData)(
          config.actorSystem.dispatcher, actor)
      )
    }
  }
  def stopSaves = schedule.foreach(_.cancel())

  def setGenHITsActiveEach(n: Int) = {
    genManager ! SetNumHITsActive(n)
  }


  import TaskManager.Message._
  def start(interval: FiniteDuration = 30 seconds) = {
    server
    startSaves()
    genActor ! Start(interval, delay = 0 seconds)
  }
  def stop() = {
    genActor ! Stop
    stopSaves
  }
  def delete() = {
    genActor ! Delete
  }
  def expire() = {
    genActor ! Expire
  }
  def update() = {
    server
    genActor ! Update
  }
  def save() = {
    accuracyTracker ! SaveData
    genManager ! SaveData
  }

  // for use while it's running. Ideally instead of having to futz around at the console calling these functions,
  // in the future you could have a nice dashboard UI that will help you examine common sources of issues

  def allGenInfos = hitDataService.getAllHITInfo[QASRLGenerationPrompt[SID], List[VerbQA]](genTaskSpec.hitTypeId).get

  def currentGenSentences: List[(SID, String)] = {
    genHelper.activeHITInfosByPromptIterator.map(_._1.id).map(id =>
      id -> Text.render(id.tokens)
    ).toList
  }


  def renderValidation(info: HITInfo[QASRLValidationPrompt[SID], List[QASRLValidationAnswer]]) = {
    val sentence = info.hit.prompt.genPrompt.id.tokens
    val genWorkerString = hitDataService
      .getAssignmentsForHIT[List[VerbQA]](genTaskSpec.hitTypeId, info.hit.prompt.sourceHITId).get
      .find(_.assignmentId == info.hit.prompt.sourceAssignmentId)
      .fold("")(_.workerId)
    Text.render(sentence) + "\n" +
      info.hit.prompt.qaPairs.zip(info.assignments.map(_.response).transpose).map {
        case (VerbQA(verbIndex, question, answers), validationAnswers) =>
          val answerString = answers.map(s => Text.renderSpan(sentence, (s.begin to s.end).toSet)).mkString(" / ")
          val validationRenderings = validationAnswers.map(QASRLValidationAnswer.render(sentence, _))
          val allValidationsString = validationRenderings.toList match {
            case Nil => ""
            case head :: tail => f"$head%20s(${tail.mkString("; ")}%s)"
          }
          f"$genWorkerString%-20s $question%-35s --> $answerString%20s | $allValidationsString"
      }.mkString("\n") + "\n"
  }

  case class StatSummary(
    workerId: String,
    numVerbs: Option[Int],
    numQs: Option[Int],
    accuracy: Option[Double],
    numAs: Option[Int],
    numInvalidAnswers: Option[Int],
    pctBad: Option[Double],
    agreement: Option[Double],
    earnings: Double)

  case class AggregateStatSummary(
    numVerbs: Int,
    numQs: Int,
    numAs: Int,
    numInvalidAnswers: Int,
    totalCost: Double) {
    def combine(worker: StatSummary) = AggregateStatSummary(
      numVerbs + worker.numVerbs.getOrElse(0),
      numQs + worker.numQs.getOrElse(0),
      numAs + worker.numAs.getOrElse(0) + worker.numInvalidAnswers.getOrElse(0),
      numInvalidAnswers + worker.numInvalidAnswers.getOrElse(0),
      totalCost + worker.earnings
    )
  }
  object AggregateStatSummary {
    def empty = AggregateStatSummary(0, 0, 0, 0, 0.0)
  }

  object StatSummary {
    def makeFromStatsAndInfo(
      stats: Option[QASRLGenerationWorkerStats],
      info: Option[QASRLValidationWorkerInfo]
    ) = stats.map(_.workerId).orElse(info.map(_.workerId)).map { wid =>
      StatSummary(
        workerId = wid,
        numVerbs = stats.map(_.numAssignmentsCompleted),
        numQs = stats.map(_.numQAPairsWritten),
        accuracy = stats.map(_.accuracy),
        numAs = info.map(i => i.numAnswerSpans + i.numInvalids),
        numInvalidAnswers = info.map(_.numInvalids),
        pctBad = info.map(_.proportionInvalid * 100.0),
        agreement = info.map(_.agreement),
        earnings = stats.fold(0.0)(_.earnings) + info.fold(0.0)(_.earnings)
      )
    }
  }

  def printStatsHeading =
    println(f"${"Worker ID"}%14s  ${"Verbs"}%5s  ${"Qs"}%5s  ${"Acc"}%4s  ${"As"}%5s  ${"%Bad"}%5s  ${"Agr"}%4s  $$")

  def printSingleStatSummary(ss: StatSummary): Unit = ss match {
    case StatSummary(wid, numVerbsOpt, numQsOpt, accOpt, numAsOpt, numInvalidsOpt, pctBadOpt, agrOpt, earnings)=>
      val numVerbs = numVerbsOpt.getOrElse("")
      val numQs = numQsOpt.getOrElse("")
      val acc = accOpt.foldMap(pct => f"$pct%.2f")
      val numAs = numAsOpt.getOrElse("")
      val pctBad = pctBadOpt.foldMap(pct => f"$pct%4.2f")
      val agr = agrOpt.foldMap(pct => f"$pct%.2f")
      println(f"$wid%14s  $numVerbs%5s  $numQs%5s  $acc%4s  $numAs%5s  $pctBad%5s  $agr%4s  $earnings%.2f")
  }

  def printCoverageStats = genManagerPeek.coverageStats.toList
    .sortBy(-_._2.size)
    .map { case (workerId, numQs) => f"$workerId%s\t${numQs.size}%d\t${numQs.sum.toDouble / numQs.size}%.2f" }
    .foreach(println)

  def printGenFeedback(n: Int) = genManagerPeek.feedbacks.take(n).foreach(a =>
    println(a.workerId + " " + a.feedback)
  )

  def printAllFeedbacks(n: Int = Int.MaxValue) = {
    println("Generation:")
    printGenFeedback(n)
  }

  def info(): Unit = {
    val totalGenPrompts = allPrompts.length * numGenerationAssignmentsForPrompt

    val completedGenerationsCount = allGenInfos.map(_.assignments.length).sum

    val uploadedGenerationsCount = allGenInfos.length

    println(s"Generation HitTypeId: ${genTaskSpec.hitTypeId}")
    println(s"Active Phase: $phase")
    println()
    println(s"Training qualification id: ${genTrainingQualType.getQualificationTypeId}")
    println(s"Production qualification id: ${genProductionQualType.getQualificationTypeId}")
    println(s"Training coverage id: ${genCoverageDisqualType.getQualificationTypeId}")
    println(s"Production accuracy id: ${genAccDisqualType.getQualificationTypeId}")
    println()
    println(f"Generation assignments: $completedGenerationsCount/$totalGenPrompts (completed / total)")
    println(f"Uploaded generation hits to MTurk: $uploadedGenerationsCount")
  }
}
