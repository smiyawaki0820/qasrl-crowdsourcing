import java.nio.file.{Path, Paths}

import example._
import qasrl.crowd._
import spacro._
import spacro.tasks._
import spacro.util._
import akka.pattern.ask

import scala.concurrent.duration._
import com.amazonaws.services.mturk._
import com.amazonaws.services.mturk.model._
import com.github.tototoshi.csv.CSVReader
import nlpdata.util.Text
import nlpdata.util.HasTokens.ops._


val isProduction = false // sandbox. change to true for production
val domain = "localhost" // change to your domain, or keep localhost for testing
val projectName = "qasrl-crowd-example" // make sure it matches the SBT project;
// this is how the .js file is found to send to the server

val interface = "0.0.0.0"
val httpPort = 8888
val httpsPort = 8080

val annotationPath = Paths.get(s"data/annotations/wikinews")
val liveDataPath = Paths.get(s"data/live/wikinews")
val qasrlPath = Paths.get(s"data/wikinews.dev.data.csv")

implicit val timeout = akka.util.Timeout(5.seconds)
implicit val config: TaskConfig = {
  if(isProduction) {
    val hitDataService = new FileSystemHITDataService(annotationPath.resolve("production"))
    ProductionTaskConfig(projectName, domain, interface, httpPort, httpsPort, hitDataService)
  } else {
    val hitDataService = new FileSystemHITDataService(annotationPath.resolve("sandbox"))
    SandboxTaskConfig(projectName, domain, interface, httpPort, httpsPort, hitDataService)
  }
}

val numGenerationsPerPrompt = 1
val numValidationsPerPrompt = 2
val numActivePrompts = 50

val setup = new AnnotationSetup(qasrlPath,
  liveDataPath,
  numGenerationsPerPrompt,
  numValidationsPerPrompt,
  numActivePrompts)

import setup.SentenceIdHasTokens

val exp = setup.experiment

def exit = {
  exp.stop()
  // actor system has to be terminated for JVM to be able to terminate properly upon :q
  config.actorSystem.terminate
  // flush & release logging resources
  import org.slf4j.LoggerFactory
  import ch.qos.logback.classic.LoggerContext
  LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext].stop
  System.out.println("Terminated actor system and logging. Type :q to end.")
}

// use with caution... intended mainly for sandbox
def deleteAll = {
  exp.setGenHITsActiveEach(0)
  exp.setValHITsActive(0)
  Thread.sleep(200)
  exp.expire
  exp.delete
}

def yesterday = {
  val cal = java.util.Calendar.getInstance
  cal.add(java.util.Calendar.DATE, -1)
  cal.getTime
}

import scala.collection.JavaConverters._

def expireHITById(hitId: String) = {
  config.service.updateExpirationForHIT(
    (new UpdateExpirationForHITRequest)
      .withHITId(hitId)
      .withExpireAt(yesterday))
}

def approveAllAssignmentsByHITId(hitId: String) = for {
  mTurkAssignment <- config.service.listAssignmentsForHIT(
    new ListAssignmentsForHITRequest()
      .withHITId(hitId)
      .withAssignmentStatuses(AssignmentStatus.Submitted)
    ).getAssignments.asScala.toList
} yield config.service.approveAssignment(
  new ApproveAssignmentRequest()
    .withAssignmentId(mTurkAssignment.getAssignmentId)
    .withRequesterFeedback(""))

def deleteHITById(hitId: String) =
  config.service.deleteHIT((new DeleteHITRequest).withHITId(hitId))

def disableHITById(hitId: String) = {
  expireHITById(hitId)
  deleteHITById(hitId)
}

def getActiveHits = {
  config.service.listAllHITs
}

def getActiveHITIds = {
  getActiveHits.map(_.getHITId)
}

def flushAll(): Unit = {
  for(hitId <- getActiveHITIds) {
    disableHITById(hitId)
  }
}

def hitInfo(hit: com.amazonaws.services.mturk.model.HIT): String = {
  val hitType = hit.getHITTypeId match {
    case exp.valTaskSpec.hitTypeId => "Validation"
    case exp.genTaskSpec.hitTypeId => "Generation"
    case _ => "UNKNOWN"
  }
  val info = s"${hit.getHITId}\t${hitType}\t${hit.getHITStatus}\t${hit.getCreationTime}"
  info
}

def printMTurkHits() = {
  val hitInfoHeading = s"id\thit_type\tstatus\tcreation_date"
  
  println(hitInfoHeading)
  for (hit <- getActiveHits) {
    val info = hitInfo(hit)
    println(info)
  }
}

def saveGenerationData(filename: String) = {
  val nonEmptyGens = exp.allGenInfos.filter(_.assignments.nonEmpty)
  setup.saveGenerationData(filename, nonEmptyGens)
}

def progress() = {
  val totalGenPrompts = exp.allPrompts.length * numGenerationsPerPrompt
  val totalValPrompts = totalGenPrompts * numValidationsPerPrompt

  val completedGenerationsCount = exp.allGenInfos.map(_.assignments.length).sum
  val completedValidationsCount = exp.allValInfos.map(_.assignments.length).sum

  val uploadedGenerationsCount = exp.allGenInfos.length
  val uploadedValidationsCount = exp.allValInfos.length

  println(s"Generation HitTypeId: ${exp.genTaskSpec.hitTypeId}")
  println(s"Validation HitTypeId: ${exp.valTaskSpec.hitTypeId}")

  println(f"Generation completed / total: $completedGenerationsCount / $totalGenPrompts ")
  println(f"Validation completed / total: $completedValidationsCount / $totalValPrompts ")

  println(f"Uploaded to MTurk: (Generation / Validation): $uploadedGenerationsCount / $uploadedValidationsCount ")
}


def savedHits(): Unit = {
  val infos = for {
    info <- exp.allValInfos
    hid = info.hit.hitId
    sid = info.hit.prompt.genPrompt.id
    vid = info.hit.prompt.genPrompt.verbIndex
    questions = info.hit.prompt.qaPairs.map(_.question)
    ass <- info.assignments
    wid = ass.workerId
    aid = ass.assignmentId
    (question, valAnswer) <- questions.zip(ass.response)
    theAnswer = QASRLValidationAnswer.render(sid.tokens, valAnswer)

  } yield f"${sid.id}\t$vid\t$question\t$theAnswer\t$wid\t$aid\t$hid"
  println("sent_id\tverb_idx\tquestion\tanswer\tworker_id\tassign_id\thit_id")
  infos.foreach(println)
}
