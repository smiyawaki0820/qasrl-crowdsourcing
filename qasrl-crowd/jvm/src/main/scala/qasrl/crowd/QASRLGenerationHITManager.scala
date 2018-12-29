package qasrl.crowd

import cats.implicits._
import spacro._
import spacro.tasks._
import upickle.default.Reader
import akka.actor.ActorRef
import com.amazonaws.services.mturk.model.{AssociateQualificationWithWorkerRequest, DisassociateQualificationFromWorkerRequest, ListWorkersWithQualificationTypeRequest}
import upickle.default._
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._


case class FlagBadSentence[SID](id: SID)

class QASRLGenerationHITManager[SID : Reader : Writer](
  helper: HITManager.Helper[QASRLGenerationPrompt[SID], List[VerbQA]],
  validationHelper: HITManager.Helper[QASRLValidationPrompt[SID], List[QASRLValidationAnswer]],
  validationActor: ActorRef,
  coverageDisqualificationTypeId: String,
  validationTempDisqualifyTypeId: String,
  // sentenceTrackingActor: ActorRef,
  numAssignmentsForPrompt: QASRLGenerationPrompt[SID] => Int,
  initNumHITsToKeepActive: Int,
  _promptSource: Iterator[QASRLGenerationPrompt[SID]])(
  implicit annotationDataService: AnnotationDataService,
  settings: QASRLSettings
) extends NumAssignmentsHITManager[QASRLGenerationPrompt[SID], List[VerbQA]](
  helper, numAssignmentsForPrompt, initNumHITsToKeepActive, _promptSource
) with StrictLogging {

  import helper._
  import config._
  import taskSpec.hitTypeId

  override def promptFinished(prompt: QASRLGenerationPrompt[SID]): Unit = {
    // sentenceTrackingActor ! GenerationFinished(prompt)
  }

  val badSentenceIdsFilename = "badSentenceIds"

  var badSentences = annotationDataService.loadLiveData(badSentenceIdsFilename)
    .map(_.mkString)
    .map(read[Set[SID]])
    .toOption.foldK

  private[this] def flagBadSentence(id: SID) = {
    badSentences = badSentences + id
    save
    for {
      (prompt, hitInfos) <- activeHITInfosByPromptIterator
      if prompt.id == id
      HITInfo(hit, _) <- hitInfos
    } yield helper.expireHIT(hit)
  }

  val coverageStatsFilename = "coverageStats"

  var coverageStats: Map[String, List[Int]] = annotationDataService.loadLiveData(coverageStatsFilename)
    .map(_.mkString)
    .map(read[Map[String, List[Int]]])
    .toOption.getOrElse(Map.empty[String, List[Int]])

  def christenWorker(workerId: String, numQuestions: Int) = {
    val newStats = numQuestions :: coverageStats.get(workerId).getOrElse(Nil)
    coverageStats = coverageStats.updated(workerId, newStats)
    val newQuestionsPerVerb = newStats.sum.toDouble / newStats.size
  }

  val feedbackFilename = "genFeedback"

  var feedbacks =
    annotationDataService.loadLiveData(feedbackFilename)
      .map(_.mkString)
      .map(read[List[Assignment[List[VerbQA]]]])
      .toOption.foldK

  private[this] def save = {
    annotationDataService.saveLiveData(
      coverageStatsFilename,
      write(coverageStats))
    annotationDataService.saveLiveData(
      feedbackFilename,
      write(feedbacks))
    annotationDataService.saveLiveData(
      badSentenceIdsFilename,
      write(badSentences))
    logger.info("Generation data saved.")
  }

  def disqualifyValidationFromWorker(workerId: String) = {
    // This may put a bit pressure on AMT, hopefully, not too much...
    // This is called every time a generator submits an assignment.
    // Every once in a few seconds at best..
    val res = config.service.listWorkersWithQualificationType(
      new ListWorkersWithQualificationTypeRequest()
        .withMaxResults(100)
        .withQualificationTypeId(validationTempDisqualifyTypeId))
    val workers = res.getQualifications.asScala.map(_.getWorkerId).toSet
    if (workers.contains(workerId)) {
      None
    } else {
      Some(config.service.associateQualificationWithWorker(
        new AssociateQualificationWithWorkerRequest()
          .withQualificationTypeId(validationTempDisqualifyTypeId)
          .withWorkerId(workerId)
          .withIntegerValue(1)
          .withSendNotification(true)))
    }
  }

  def onStop() = {
    val res = config.service.listWorkersWithQualificationType(
      new ListWorkersWithQualificationTypeRequest()
        .withMaxResults(100)
        .withQualificationTypeId(validationTempDisqualifyTypeId))
    for (q <- res.getQualifications().asScala){
      config.service.disassociateQualificationFromWorker(
        new DisassociateQualificationFromWorkerRequest()
          .withWorkerId(q.getWorkerId)
          .withQualificationTypeId(validationTempDisqualifyTypeId)
          .withReason("Restored your ability to validate answers. " +
            "Thank you for participating.")
      )

    }
  }

  override def reviewAssignment(hit: HIT[QASRLGenerationPrompt[SID]], assignment: Assignment[List[VerbQA]]): Unit = {
    evaluateAssignment(hit, startReviewing(assignment), Approval(""))
    if(!assignment.feedback.isEmpty) {
      feedbacks = assignment :: feedbacks
      logger.info(s"Feedback: ${assignment.feedback}")
    }

    val newQuestionRecord = assignment.response.size :: coverageStats.get(assignment.workerId).foldK
    coverageStats = coverageStats.updated(assignment.workerId, newQuestionRecord)
    val verbsCompleted = newQuestionRecord.size
    val questionsPerVerb = newQuestionRecord.sum.toDouble / verbsCompleted
    if(questionsPerVerb < settings.generationCoverageQuestionsPerVerbThreshold &&
         verbsCompleted > settings.generationCoverageGracePeriod) {
      config.service.associateQualificationWithWorker(
        new AssociateQualificationWithWorkerRequest()
          .withQualificationTypeId(coverageDisqualificationTypeId)
          .withWorkerId(assignment.workerId)
          .withIntegerValue(1)
          .withSendNotification(true))
    }

    disqualifyValidationFromWorker(assignment.workerId)

    val validationPrompt = QASRLValidationPrompt(hit.prompt, hit.hitTypeId, hit.hitId, assignment.assignmentId, assignment.response)
    validationActor ! validationHelper.Message.AddPrompt(validationPrompt)
    // sentenceTrackingActor ! ValidationBegun(validationPrompt)
  }

  override lazy val receiveAux2: PartialFunction[Any, Unit] = {
    case SaveData => save
    case Pring => println("Generation manager pringed.")
    case fbs: FlagBadSentence[SID] => fbs match {
      case FlagBadSentence(id) => flagBadSentence(id)
    }
    case ChristenWorker(workerId, numQuestions) =>
      christenWorker(workerId, numQuestions)
  }
}
