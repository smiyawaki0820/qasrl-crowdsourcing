package example


import java.nio.file.Paths
import java.util.Random

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import com.typesafe.scalalogging.StrictLogging
import nlpdata.datasets.wiktionary.{InflectedForms, Inflections, WiktionaryFileSystemService}
import nlpdata.util.LowerCaseStrings._
import qasrl.labeling.SlotBasedLabel
import qasrl.{QuestionProcessor, TemplateStateMachine}
import qasrl.Frame
import qasrl.util.implicits._
import cats.implicits._
import cats.data.NonEmptyList

import scala.collection.mutable

case class QA(qasrlId: String, verbIdx: Int, verb: String, question: String, sourceId: String)

object RunQuestionParser extends App with StrictLogging{

  val qasrlPredictedPath = Paths.get("./data/", "wikinews.dev.qasrl.mult_gen.csv")
//  val qasrlOutputPath = Paths.get("./data/","wikinews.dev.qasrl.mult_gen.questions.csv")
  val sentencesPath = Paths.get("./data", "wikinews.dev.data.csv")

  val recordsReader = CSVReader.open(qasrlPredictedPath.toString)
  val sentReader = CSVReader.open(sentencesPath.toString)

//  val recordsWriter = CSVWriter.open(qasrlOutputPath.toString)

  // Wiktionary data contains a bunch of inflections, used for the main verb in the QA-SRL template
  val wiktionary = new WiktionaryFileSystemService(Paths.get("datasets/wiktionary"))

  val sentMap: Map[String, Vector[String]] = sentReader.allWithHeaders().map(rec =>
    rec("qasrl_id") -> rec("tokens").split(" ").toVector).toMap

  val records: Vector[QA] = (for (rec <- recordsReader.allWithHeaders())
      yield QA(rec("qasrl_id"),
        rec("verb_idx").toInt,
        rec("verb"),
        rec("question"),
        rec("source_assign_id"))).toVector

  // Header for CSV that would be printed on screen
  println("qasrl_id,verb_idx,question,source_assign_id,wh,subj,aux,verb_prefix,verb,obj,prep,obj2," +
    "is_passive,is_negated,is_progressive,is_perfect")

  for ((qasrlId, sentRecords) <- records.groupBy(_.qasrlId)) {
    val tokens = sentMap(qasrlId)
    val inflections = wiktionary.getInflectionsForTokens(tokens.iterator)
    for ((verb_idx, predRecords) <- sentRecords.groupBy(_.verbIdx)) {
      val verb = tokens(verb_idx)

      val verbInflectedForms = inflections.getInflectedForms(verb.lowerCase).get
      for ((sourceId, q) <- predRecords.map(r => (r.sourceId, r.question)).distinct) {
        val qTokens = q.init.split(" ").toVector.map(_.lowerCase)
        val qPreps = qTokens.filter(TemplateStateMachine.allPrepositions.contains).toSet
        val qPrepBigrams = qTokens.sliding(2)
          .filter(_.forall(TemplateStateMachine.allPrepositions.contains))
          .map(_.mkString(" ").lowerCase)
          .toSet

        val stateMachine = new TemplateStateMachine(
          tokens,
          verbInflectedForms,
          Some(qPreps ++ qPrepBigrams))
        val template = new QuestionProcessor(stateMachine)

        val goodStatesOpt = template.processStringFully(q).toOption
        val slots = SlotBasedLabel.getSlotsForQuestion(tokens, verbInflectedForms, List(q))
        for {
          slotOpt <- slots
          slot <- slotOpt
          goodStates <- goodStatesOpt

        } {
          val frame: Frame = goodStates.toList.collect {
            case QuestionProcessor.CompleteState(_, someFrame, _) => someFrame
          }.head
          val subj = slot.subj.getOrElse("")
          val aux = slot.aux.getOrElse("")
          val verbPrefix = slot.verbPrefix
          val obj = slot.obj.getOrElse("")
          val prep = slot.prep.getOrElse("")
          val obj2 = slot.obj2.getOrElse("")
          println(s"$qasrlId,$verb_idx,$q,$sourceId,${slot.wh},$subj,$aux,$verbPrefix,${slot.verb},$obj,$prep,$obj2," +
            s"${frame.isPassive},${frame.isNegated},${frame.isProgressive},${frame.isPerfect}")
        }
      }
    }
  }
}
