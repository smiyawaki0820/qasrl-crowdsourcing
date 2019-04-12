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

  // Uncomment the phase you want to activate
  //val phase = Trap
  val phase = Training
  //val phase = Production
  val phaseName = phase.toString.toLowerCase
  val annotationPath = Paths.get(s"data/annotations.$phaseName")
  val liveDataPath = Paths.get(s"data/live.$phaseName")
  val sentsPath = Paths.get(s"data/$phaseName.csv")
  val qasrlPath = Paths.get(s"data/$phaseName.annot.csv")

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
  val setup = new EvaluationSetup(qasrlPath, sentsPath, liveDataPath)

  import setup.SentenceIdHasTokens

  val exp = setup.experiment
  exp.start()

}
