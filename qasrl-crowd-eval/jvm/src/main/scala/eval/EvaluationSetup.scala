package eval

import java.nio.file.{Files, Path, Paths}

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import com.typesafe.scalalogging.StrictLogging
import example.Tokenizer
import nlpdata.datasets.wiktionary
import nlpdata.util.HasTokens
import nlpdata.util.HasTokens.ops._
import nlpdata.util.HasTokens
import qasrl.crowd._
import spacro.HITInfo
import spacro.tasks.TaskConfig
import spacro.util.Span

import scala.collection.immutable
import scala.util.Try
import scala.collection.JavaConverters._
import scala.util.Random

case class QA[SID](sentenceId: SID, verbIndex: Int, question: String, answers: List[Span], assignId: String)

class EvaluationSetup(qasrlPath: Path,
                      datasetPath: Path,
                      liveDataPath: Path,
                      phase: Phase,
                      numEvaluationAssignmentsForPrompt: Int)(
                       implicit config: TaskConfig) extends StrictLogging {

  val resourcePath = java.nio.file.Paths.get("datasets")
  val staticDataPath = Paths.get(s"data/static")

  implicit val liveAnnotationDataService = new FileSystemAnnotationDataService(liveDataPath)

  def saveOutputFile(name: String, contents: String): Try[Unit] = Try {
    val path = staticDataPath.resolve("out").resolve(name)
    val directory = path.getParent
    if (!Files.exists(directory)) {
      Files.createDirectories(directory)
    }
    Files.write(path, contents.getBytes())
  }

  def loadOutputFile(name: String): Try[List[String]] = Try {
    val path = staticDataPath.resolve("out").resolve(name)
    import scala.collection.JavaConverters._
    Files.lines(path).iterator.asScala.toList
  }

  private def decodeAnswerRange(answerRange: String): List[Span] = {
    if (answerRange.isEmpty)
      return List.empty[Span]

    val encodedSpans: Vector[String] = answerRange.split("~!~").toVector
    val spans = encodedSpans.map(encSpan => {
      val splits = encSpan.split(':').toVector
      // this project uses exclusive indices
      Span(splits(0).toInt, splits(1).toInt - 1)
    })
    spans.toList
  }

  private def getVerbQas(verbIdx: Int, qaPairs: Vector[QA[SentenceId]]): List[(String, VerbQA)] = {
    // qaPairs already belong to a single <sentence, verb>,
    // but may come from multiple assignments
    val sortedQas = qaPairs.sortBy(_.question.split(" ")(0))
    val qasAnsAssigns = for {
      qa <- sortedQas
    } yield (qa.assignId, VerbQA(verbIdx, qa.question, qa.answers))
    qasAnsAssigns.toList
  }

  private def getValidationPrompts(qaPairsCsvPath: Path, dataset: Map[String, Vector[String]]): Vector[QASRLArbitrationPrompt[SentenceId]] = {
    val allRecords = CSVReader.open(qaPairsCsvPath.toString).allWithHeaders()
    val qaPairs = (for {
      rec <- allRecords
      sentId = rec("qasrl_id")
      sent = dataset(sentId)
      verbIdx = rec("verb_idx").toInt
      assignId = rec("assign_id")
      question = rec("question")
      answerRanges = rec("answer_range")
      spans = decodeAnswerRange(answerRanges)
    } yield new QA[SentenceId](SentenceId(sentId), verbIdx, question, spans, assignId)).toVector

    val valPrompts = for {
      (key, qaGroup) <- qaPairs.groupBy(qa => (qa.sentenceId, qa.verbIndex))
      (sentId, verbIdx) = key
      assigns = qaGroup.map(_.assignId).distinct
      randAssigns = Random.shuffle(assigns).take(2)
      qa_group_in_combination = qaGroup.filter(qa => randAssigns.contains(qa.assignId))
      qasAndIds = getVerbQas(verbIdx, qa_group_in_combination)
      genPrompt = QASRLGenerationPrompt[SentenceId](sentId, verbIdx)
    } yield QASRLArbitrationPrompt[SentenceId](genPrompt, qasAndIds)
    valPrompts.toVector
  }

  val dataset: Map[String, Vector[String]] = {
    logger.info(s"Reading dataset from: $datasetPath")
    val reader: CSVReader = CSVReader.open(datasetPath.toString)
    // CSV format:
    // qasrl_id, tokens, sentence
    // 10_13ecbplus.xml_0,Report : Red Sox offer Teixeira $ 200 million,Report: Red Sox offer Teixeira $200 million
    val csvRecords = reader.allWithHeaders()
    (for {
      rec <- csvRecords
      id = rec("qasrl_id")
      tokens: Vector[String] = rec("tokens").split(" ").toVector
    } yield id -> tokens).toMap
  }

  val allIds: Vector[SentenceId] = dataset.keys.map(SentenceId(_)).toVector

  lazy val Wiktionary = new wiktionary.WiktionaryFileSystemService(
    resourcePath.resolve("wiktionary")
  )

  implicit object SentenceIdHasTokens extends HasTokens[SentenceId] {
    override def getTokens(sid: SentenceId): Vector[String] = dataset(sid.id)
  }

  implicit lazy val inflections = {
    val tokens = for {
      id <- allIds.iterator
      word <- id.tokens.iterator
    } yield word
    Wiktionary.getInflectionsForTokens(tokens)
  }

  // use qasrlPath as CSV Path for QA pairs
  val allPrompts: Vector[QASRLArbitrationPrompt[SentenceId]] = getValidationPrompts(qasrlPath, dataset)
  val experiment = new QASRLEvaluationPipeline[SentenceId](
    allPrompts,
    numEvaluationAssignmentsForPrompt,
    phase)

  val exp = experiment

  val qasrlColumns = List(
    "qasrl_id", "verb_idx", "verb",
    "worker_id", "assign_id", "source_assign_id",
    "question", "is_redundant", "answer_range", "answer",
    "wh", "subj", "obj", "obj2", "aux", "prep", "verb_prefix",
    "is_passive", "is_negated")

  def saveArbitrationData(filename: String,
                          valInfos: List[HITInfo[QASRLArbitrationPrompt[SentenceId], List[QASRLValidationAnswer]]]): Unit = {
    val contents: List[QASRL] = DataIO.makeArbitrationQAPairTSV(SentenceId.toString, valInfos).toList
    val path = liveDataPath.resolve(filename).toString
    val csv = CSVWriter.open(path, encoding = "utf-8")
    csv.writeRow(qasrlColumns)
    for (qasrl <- contents) {
      // will iterate in order over the case class fields
      csv.writeRow(qasrl.productIterator.toList)
    }
  }

}
