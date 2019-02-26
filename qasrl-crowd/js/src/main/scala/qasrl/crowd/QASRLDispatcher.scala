package qasrl.crowd

import spacro.tasks._

import scalajs.js
import scalajs.js.JSApp
import org.scalajs.jquery.jQuery
import upickle.default._
import japgolly.scalajs.react.vdom.html_<^.VdomTag

abstract class QASRLDispatcher[SID : Reader : Writer](implicit settings: QASRLSettings) extends TaskDispatcher {

  def generationInstructions(generationTaskKey: Phase): VdomTag
  def validationInstructions: VdomTag

//  lazy val genClient = new QASRLGenerationClient[SID](generationInstructions(""))


  lazy val genTrapClient = new QASRLGenerationSimplifiedClient[SID](generationInstructions(Trap))
  lazy val genTrainClient = new QASRLGenerationSimplifiedClient[SID](generationInstructions(Training))
  lazy val genProdClient = new QASRLGenerationSimplifiedClient[SID](generationInstructions(Production))

  lazy val valClient = new QASRLValidationClient[SID](validationInstructions)

  final override lazy val taskMapping = Map[String, () => Unit](
//    settings.generationTaskKey -> genClient.main,
    settings.generationTrapTaskKey -> genTrapClient.main,
    settings.generationTrainTaskKey -> genTrainClient.main,
    settings.generationProdTaskKey -> genProdClient.main,
    settings.validationTaskKey -> valClient.main)

}
