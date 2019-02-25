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
import scala.collection.JavaConverters._

val isProduction = false // sandbox. change to true for production
val domain = "localhost" // change to your domain, or keep localhost for testing
val projectName = "qasrl-crowd-example" // make sure it matches the SBT project;
// this is how the .js file is found to send to the server

val interface = "0.0.0.0"
val httpPort = 8888
val httpsPort = 8080

// Uncomment the phase you want to activate
//val phase = Trap
val phase = Training
//val phase = Production
val phaseName = phase.toString.toLowerCase
val annotationPath = Paths.get(s"data/annotations/wikinews.{$phaseName}")
val liveDataPath = Paths.get(s"data/live.{$phaseName}")
val qasrlPath = Paths.get(s"data/wikinews.dev.{$phaseName}.csv")



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
val numActivePrompts = 50

val setup = new AnnotationSetup(qasrlPath,
  liveDataPath, numGenerationsPerPrompt,
  numActivePrompts,
  phase)

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
  Thread.sleep(200)
  exp.expire
  exp.delete
}


def saveGenerationData(filename: String) = {
  val nonEmptyGens = exp.allGenInfos.filter(_.assignments.nonEmpty)
  setup.saveGenerationData(filename, nonEmptyGens)
}
