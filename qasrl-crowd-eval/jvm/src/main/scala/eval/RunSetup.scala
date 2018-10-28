package eval

import java.nio.file.{Path, Paths}

import eval._
import qasrl.crowd.{QASRLValidationAnswer, _}
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

object RunSetup extends App {
  val isProduction = false // sandbox. change to true for production
  val domain = "localhost" // change to your domain, or keep localhost for testing
  val projectName = "qasrl-crowd-eval" // make sure it matches the SBT project;
  // this is how the .js file is found to send to the server

  val interface = "0.0.0.0"
  val httpPort = 8888
  val httpsPort = 8080

  val BATCH_NUMBER = 0

  val annotationPath = Paths.get(s"data/annotations/ecb_${BATCH_NUMBER}")
  val liveDataPath = Paths.get(s"data/live/ecb_${BATCH_NUMBER}")
  val qasrlPath = Paths.get(s"data/ecb/ECBPlus.qasrl.batch_${BATCH_NUMBER}.csv")

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
  val genTypeId = "./data/nrl_generated_0.2.csv"
  val setup = new EvaluationSetup(genTypeId, qasrlPath, liveDataPath)

  import setup.SentenceIdHasTokens

  val exp = setup.experiment
  exp.start()

}
