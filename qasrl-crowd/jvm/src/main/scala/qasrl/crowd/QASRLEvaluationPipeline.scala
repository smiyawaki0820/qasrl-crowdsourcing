package qasrl.crowd

import qasrl.crowd.util.PosTagger
import qasrl.crowd.util.implicits._
import qasrl.labeling.SlotBasedLabel
import cats.implicits._
import akka.actor._
import akka.stream.scaladsl.{Flow, Source}
import com.amazonaws.services.mturk.model._
import nlpdata.structure._
import nlpdata.util.HasTokens
import nlpdata.util.HasTokens.ops._
import nlpdata.util.Text
import nlpdata.util.PosTags
import nlpdata.util.LowerCaseStrings._
import nlpdata.datasets.wiktionary.Inflections
import spacro._
import spacro.tasks._
import spacro.util.Span
import upickle.default._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.StrictLogging

class QASRLEvaluationPipeline[SID : Reader : Writer : HasTokens](
  val allPrompts: Vector[QASRLArbitrationPrompt[SID]], // IDs of sentences to annotate
  val numValidationsForPrompt: Int,
  phase: Phase,
  frozenEvaluationHITTypeId: Option[String] = None,
  validationAgreementDisqualTypeLabel: Option[String] = None,
  alternativePromptReaderOpt: Option[Reader[QASRLArbitrationPrompt[SID]]] = None)(
  implicit val config: TaskConfig,
  val annotationDataService: AnnotationDataService,
  val settings: QASRLEvaluationSettings,
  val inflections: Inflections
) extends StrictLogging {

  import config.hitDataService

  val qual = new QualificationService()

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

  val valAgrDisqualTypeLabelString = validationAgreementDisqualTypeLabel.fold("")(x => s"[$x] ")
  val valAgrDisqualTypeName = s"${valAgrDisqualTypeLabelString}Question answering agreement disqualification"
  val valAgrDisqualType = config.service.listQualificationTypes(
    new ListQualificationTypesRequest()
      .withQuery(valAgrDisqualTypeName)
      .withMustBeOwnedByCaller(true)
      .withMustBeRequestable(false)
      .withMaxResults(100)
  ).getQualificationTypes.asScala.toList.find(_.getName == valAgrDisqualTypeName).getOrElse {
    System.out.println("Generating validation disqualification type...")
    config.service.createQualificationType(
      new CreateQualificationTypeRequest()
        .withName(valAgrDisqualTypeName)
        .withKeywords("language,english,question answering")
        .withDescription("""Agreement with other annotators on answers and validity judgments
          in our question answering task is too low.""".replaceAll("\\s+", " "))
        .withQualificationTypeStatus(QualificationTypeStatus.Active)
        .withAutoGranted(false)
    ).getQualificationType
  }
  val valAgrDisqualTypeId = valAgrDisqualType.getQualificationTypeId
  val valAgreementRequirement = new QualificationRequirement()
    .withQualificationTypeId(valAgrDisqualTypeId)
    .withComparator("DoesNotExist")
    .withRequiredToPreview(false)

  val productionQualName = "Production Phase for annotators in question generation"
  val productionQualType = qual.findOrCreate(productionQualName, """Access granted to the live annotation round in writing questions and answers""".stripMargin)
  val inProductionReq = qual.createQualificationReq(productionQualType, shouldHave = true)

  // Currently, no training phase for arbitration task.
  val InProductionGroupReq = phase match {
    case Production(groupId) => qual.createQualificationReq(productionQualType, true, groupId)
    case _ => throw new IllegalArgumentException(phase.toString)
  }


  // NOTE may need to call multiple times to cover all workers... sigh TODO pagination
  def resetAllQualificationValues = {
    def revokeAllWorkerQuals(qualTypeId: String) = {
      val quals = config.service.listWorkersWithQualificationType(
        new ListWorkersWithQualificationTypeRequest()
          .withQualificationTypeId(qualTypeId)
          .withMaxResults(100)
      ).getQualifications.asScala.toList
      quals.foreach(qual =>
        config.service.disassociateQualificationFromWorker(
          new DisassociateQualificationFromWorkerRequest()
            .withQualificationTypeId(qualTypeId)
            .withWorkerId(qual.getWorkerId)
        )
      )
    }
    revokeAllWorkerQuals(valAgrDisqualTypeId)
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

  // validation task definition

  val valHITType = HITType(
    title = s"Consolidate Question-Answer pairs about a verb",
    description = s"""
      Given a sentence, a verb, and several Q&A pairs collected from different annotators,
      You should identify the most naturally phrased question in English
      for each subset of questions that ask about the same thing, and highlight all of its answers.
      You should then mark the other valid but redundant questions as redundant.
      Questions that are ungrammatical, not related to the verb, or that have no direct
      answer in the text should be marked as invalid.
      You should delete incorrect answers, modify existing ones if needed, and add new answers if missing.
      Maintain high agreement with our expert annotator team to stay qualified.
    """.trim,
    reward = settings.arbitrationReward,
    keywords = "language,english,question answering",
    qualRequirements = Array[QualificationRequirement](
      approvalRateRequirement, localeRequirement, valAgreementRequirement, inProductionReq, InProductionGroupReq),
    autoApprovalDelay = 2592000L, // 30 days
    assignmentDuration = 600L)

  lazy val valAjaxService = new Service[QASRLValidationAjaxRequest[SID]] {
    override def processRequest(request: QASRLValidationAjaxRequest[SID]) = request match {
      case QASRLValidationAjaxRequest(workerIdOpt, id) =>
        val workerInfoSummaryOpt = for {
          valManagerP <- Option(valManagerPeek)
          workerId <- workerIdOpt
          info <- valManagerP.allWorkerInfo.get(workerId)
        } yield info.summary

        QASRLValidationAjaxResponse(workerInfoSummaryOpt, id.tokens)
    }
  }

  lazy val sampleValPrompt = allPrompts.head

  lazy val valTaskSpec = TaskSpecification.NoWebsockets[
    QASRLArbitrationPrompt[SID], List[QASRLValidationAnswer], QASRLValidationAjaxRequest[SID]](
    settings.evaluationTaskKey, valHITType, valAjaxService, Vector(sampleValPrompt),
    taskPageHeadElements = taskPageHeadLinks,
    taskPageBodyElements = taskPageBodyLinks,
    frozenHITTypeId = frozenEvaluationHITTypeId)

  import config.actorSystem

  var valManagerPeek: QASRLEvaluationHITManager[SID] = null

  lazy val valHelper = new HITManager.Helper(valTaskSpec)
  lazy val valManager: ActorRef = actorSystem.actorOf(
    Props {
      valManagerPeek = new QASRLEvaluationHITManager(
        valAgrDisqualTypeId,
        valHelper,
        if(config.isProduction) (_ => numValidationsForPrompt) else (_ => 1),
        if(config.isProduction) 100 else 20,
        allPrompts.iterator)
      valManagerPeek
    })

  lazy val valActor = actorSystem.actorOf(Props(new TaskManager(valHelper, valManager)))

  lazy val server = new Server(List(valTaskSpec))

  // used to schedule data-saves
  private[this] var schedule: List[Cancellable] = Nil
  def startSaves(interval: FiniteDuration = 5 minutes): Unit = {
    if(schedule.exists(_.isCancelled) || schedule.isEmpty) {
      schedule = List(valManager).map(actor =>
        config.actorSystem.scheduler.schedule(
          2 seconds, interval, actor, SaveData)(
          config.actorSystem.dispatcher, actor)
      )
    }
  }
  def stopSaves = schedule.foreach(_.cancel())

  def setValHITsActive(n: Int) = {
    valManager ! SetNumHITsActive(n)
  }

  import TaskManager.Message._
  def start(interval: FiniteDuration = 30 seconds) = {
    server
    logger.info(s"Evaluation HitTypeId: ${valTaskSpec.hitTypeId}")
    startSaves()
    valActor ! Start(interval, delay = 3 seconds)
  }
  def stop() = {
    valActor ! Stop
    stopSaves
  }
  def delete() = {
    valActor ! Delete
  }
  def expire() = {
    valActor ! Expire
  }
  def update() = {
    server
    valActor ! Update
  }
  def save() = {
    valManager ! SaveData
  }

  // for use while it's running. Ideally instead of having to futz around at the console calling these functions,
  // in the future you could have a nice dashboard UI that will help you examine common sources of issues

  def allInfos = alternativePromptReaderOpt match {
    case None =>
      hitDataService.getAllHITInfo[QASRLArbitrationPrompt[SID], List[QASRLValidationAnswer]](valTaskSpec.hitTypeId).get
    case Some(altReader) =>
      hitDataService.getAllHITInfo[QASRLArbitrationPrompt[SID], List[QASRLValidationAnswer]](
        valTaskSpec.hitTypeId
      )(altReader, implicitly[Reader[List[QASRLValidationAnswer]]]).get
  }

  def latestInfos(n: Int = 5) = allInfos
    .filter(_.assignments.nonEmpty)
    .sortBy(_.assignments.map(_.submitTime).max)
    .takeRight(n)

  // sorted increasing by submit time
  def infosForWorker(workerId: String) = {
    val scored = for {
      hi <- allInfos
      if hi.assignments.exists(_.workerId == workerId)
      workerAssignment = hi.assignments.find(_.workerId == workerId).get
      nonWorkerAssignments = hi.assignments.filter(_.workerId != workerId)
    } yield (HITInfo(hi.hit, workerAssignment :: nonWorkerAssignments), workerAssignment.submitTime)
    scored.sortBy(_._2).map(_._1)
  }

  case class StatSummary(
    workerId: String,
    numAs: Option[Int],
    numInvalidAnswers: Option[Int],
    pctBad: Option[Double],
    agreement: Option[Double],
    hardAgreement: Option[Double],
    earnings: Double)

  case class AggregateStatSummary(
    numAs: Int,
    numInvalidAnswers: Int,
    totalCost: Double) {
    def combine(worker: StatSummary) = AggregateStatSummary(
      numAs + worker.numAs.getOrElse(0) + worker.numInvalidAnswers.getOrElse(0),
      numInvalidAnswers + worker.numInvalidAnswers.getOrElse(0),
      totalCost + worker.earnings
    )
  }
  object AggregateStatSummary {
    def empty = AggregateStatSummary(0, 0, 0.0)
  }

  object StatSummary {
    def makeFromInfo(
      info: Option[QASRLValidationWorkerInfo]
    ) = info.map(_.workerId).map { wid =>
      StatSummary(
        workerId = wid,
        numAs = info.map(i => i.numAnswerSpans + i.numInvalids),
        numInvalidAnswers = info.map(_.numInvalids),
        pctBad = info.map(_.proportionInvalid * 100.0),
        hardAgreement = info.map(_.hardAgreement),
        agreement = info.map(_.agreement),
        earnings = info.fold(0.0)(_.earnings)
      )
    }
  }

  def allStatSummaries = {
    val allInfos = valManagerPeek.allWorkerInfo
    allInfos.keySet.toList.flatMap((wid: String) =>
      StatSummary.makeFromInfo(allInfos.get(wid))
    )
  }

  def printStatsHeading =
    println(f"${"Worker ID"}%14s  ${"As"}%5s  ${"%Bad"}%5s  ${"Agr"}%4s  ${"HAgr"}%4s  $$")

  def printSingleStatSummary(ss: StatSummary): Unit = ss match {
    case StatSummary(wid, numAsOpt, numInvalidsOpt, pctBadOpt, agrOpt, hardAgrOpt, earnings)=>
      val numAs = numAsOpt.getOrElse("")
      val pctBad = pctBadOpt.foldMap(pct => f"$pct%4.2f")
      val agr = agrOpt.foldMap(pct => f"$pct%.2f")
      val hardAgr = hardAgrOpt.foldMap(pct => f"$pct%.2f")
      println(f"$wid%14s  $numAs%5s  $pctBad%5s  $agr%4s  $hardAgr%4s  $earnings%.2f")
  }

  def statsForWorker(workerId: String): Option[StatSummary] = allStatSummaries.find(_.workerId == workerId)

  def printStatsForWorker(workerId: String) = statsForWorker(workerId) match {
    case None => println("No stats for worker.")
    case Some(ss) =>
      printStatsHeading
      printSingleStatSummary(ss)
  }

  def printStats[B : Ordering](sortFn: StatSummary => B) = {
    val summaries = allStatSummaries.sortBy(sortFn)
    printStatsHeading
    summaries.foreach(printSingleStatSummary)
  }

  def printAllStats = printStats(-_.numAs.getOrElse(0))

  def printFeedbacks(n: Int = 15) = valManagerPeek.feedbacks.take(n).foreach(a =>
    println(a.workerId + " " + a.feedback)
  )

  def aggregateStats = allStatSummaries.foldLeft(AggregateStatSummary.empty)(_ combine _)

  def printAggregateStats = aggregateStats match {
    case AggregateStatSummary(numAs, numInvalidAnswers, totalCost) =>
      println(f"${"Num answers:"}%-20s$numAs%d")
      println(f"${"Num invalids:"}%-20s$numInvalidAnswers%d")
      println(f"${"Total cost:"}%-20s$totalCost%.2f")
  }

  def info(): Unit = {
    val totalPrompts = allPrompts.length * numValidationsForPrompt

    val completedCount = allInfos.map(_.assignments.length).sum
    val uploadedCount = allInfos.length

    println(s"Arbitration HitTypeId: ${valTaskSpec.hitTypeId}")
    println(s"Active Phase: $phase")
    phase match {
      case Production(groupId) => println(s"Group: $groupId")
      case _ =>
    }
    println()
    println(s"Production Qualification Id: ${productionQualType.getQualificationTypeId}")
    println(s"Validator Disqualification Id: $valAgrDisqualTypeId")
    println()
    println(f"Assignments: $completedCount/$totalPrompts (completed / total)")
    println(f"Uploaded generation hits to MTurk: $uploadedCount")
  }
}
