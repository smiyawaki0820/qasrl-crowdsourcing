package example

import java.nio.file.Paths
import java.util.Random

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import com.typesafe.scalalogging.StrictLogging
import nlpdata.datasets.wiktionary.{InflectedForms, Inflections, WiktionaryFileSystemService}
import nlpdata.util.LowerCaseStrings._
import qasrl.{QuestionProcessor, TemplateStateMachine}

import scala.collection.mutable

object RunQuestionValidation extends App with StrictLogging{

  val qasrlPredictedPath = Paths.get("./data/", "ECBPlus.pred.thresh_0.2.csv")
  val qasrlOutputPath = Paths.get("./data/","ECBPlus.pred.thresh_0.2_question_validation.csv")
//  val qasrlPredictedPath = Paths.get("./data/", "test.csv")
//  val qasrlOutputPath = Paths.get("./data/","test.question_validation.csv")

  val recordsReader = CSVReader.open(qasrlPredictedPath.toString)
  val recordsWriter = CSVWriter.open(qasrlOutputPath.toString)

  // Wiktionary data contains a bunch of inflections, used for the main verb in the QA-SRL template
  val wiktionary = new WiktionaryFileSystemService(Paths.get("datasets/wiktionary"))

  val records = recordsReader.allWithHeaders()
  val sentences = records.map(rec => rec("sentence")).distinct

  val inflectionMap = mutable.Map.empty[String, Inflections]
  val keys = mutable.ListBuffer.empty[String]
  def validate(tokens: Vector[String], question: String, inflectedForms: InflectedForms): Boolean = {
    // State machine stores all of the logic of QA-SRL templates and
    // connects them to / iteratively constructs their Frames (see Frame.scala)
    val stateMachine = new TemplateStateMachine(tokens, inflectedForms)
    val questionProcessor = new QuestionProcessor(stateMachine)
    questionProcessor.isValid(question)
  }

  def validateWithCache(sentence: String, verb: String, question: String): Boolean = {
    val tokens: Vector[String] = sentence.split(" ").toVector
    if (inflectionMap.size > 10) {

      val r = new Random()
      val idxToRemove = r.nextInt(inflectionMap.size)
      val keyToRemove = keys(idxToRemove)
      if (keyToRemove != sentence) {
        inflectionMap.remove(keyToRemove)
        keys.remove(idxToRemove)
      }
    }
    if (!inflectionMap.contains(sentence)) {
      keys += sentence
    }
    // Inflections object stores inflected forms for all of the verb
    // tokens seen in exampleSentence.iterator
    val inflections = inflectionMap.getOrElseUpdate(sentence,
      wiktionary.getInflectionsForTokens(tokens.iterator))
    // Inflected forms object holds stem, present, past, past-participle,
    // and present-participle forms
    val verbInflectedForms = inflections.getInflectedForms(verb.lowerCase)
    val isValid = verbInflectedForms match {
      case Some(inflectedForms) => validate(tokens, question, inflectedForms)
      case None => false
    }
    isValid
  }

  recordsWriter.writeRow(Vector("ecb_id", "verb", "verb_idx", "question", "answer", "is_grammatical"))

  records.foreach(rec => {
    val sentence = rec("sentence")
    val verb = rec("verb")
    val question = rec("question")
    val answer = rec("answer")
    val isValid = validateWithCache(sentence, verb, question)
    val newRecord = Vector(
      rec("ecb_id"),
      verb,
      rec("verb_idx"),
      question,
      rec("answer"),
      isValid)
    recordsWriter.writeRow(newRecord)
  })


  recordsReader.close()
  recordsWriter.close()


  recordsReader.allWithHeaders()


}
