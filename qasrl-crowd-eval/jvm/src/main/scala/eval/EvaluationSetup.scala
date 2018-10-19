package eval

import java.nio.file.{Files, Path, Paths}

import com.github.tototoshi.csv.CSVReader
import com.typesafe.scalalogging.StrictLogging
import example.Tokenizer
import nlpdata.datasets.wiktionary
import nlpdata.util.HasTokens
import nlpdata.util.HasTokens.ops._
import nlpdata.util.HasTokens
import qasrl.crowd.{FileSystemAnnotationDataService, QASRLEvaluationPipeline, QASRLEvaluationPrompt, QASRLGenerationPrompt, QASRLValidationPrompt, SourcedQuestion, VerbQA}
import spacro.HITInfo
import spacro.tasks.TaskConfig
import spacro.util.Span

import scala.collection.immutable
import scala.util.Try


case class QA[SID](sentenceId: SID, verbIndex: Int, question: String, answer: Span)

class EvaluationSetup(genTypeId: String,
                      datasetPath: Path,
                      liveDataPath: Path)(
  implicit config: TaskConfig) extends StrictLogging{

  val resourcePath = java.nio.file.Paths.get("datasets")
  val staticDataPath = Paths.get(s"data/static")

  implicit val liveAnnotationDataService = new FileSystemAnnotationDataService(liveDataPath)

  def saveOutputFile(name: String, contents: String): Try[Unit] = Try {
    val path = staticDataPath.resolve("out").resolve(name)
    val directory = path.getParent
    if(!Files.exists(directory)) {
      Files.createDirectories(directory)
    }
    Files.write(path, contents.getBytes())
  }

  def loadOutputFile(name: String): Try[List[String]] = Try {
    val path = staticDataPath.resolve("out").resolve(name)
    import scala.collection.JavaConverters._
    Files.lines(path).iterator.asScala.toList
  }

  private def flattenHitInfos[SID](hitInfos: List[HITInfo[QASRLGenerationPrompt[SID], List[VerbQA]]]) = {
    val flats: Vector[QA[SID]] = (for {
      hitInfo <- hitInfos
      sid = hitInfo.hit.prompt.id
      verbIndex = hitInfo.hit.prompt.verbIndex
      assignment <- hitInfo.assignments
      resp <- assignment.response
      question = resp.question
      answer <- resp.answers
    } yield QA(sid, verbIndex, question, answer)).toVector
    flats
  }

  private def createEvaluationPrompt[SID](sentenceId: SID,
                             flats: Vector[QA[SID]])
  :QASRLEvaluationPrompt[SID] = {
    // verbIndex, question
    val sourceQuestions = for {
      ((verbIndex, question), qas) <- flats.groupBy(f => (f.verbIndex, f.question))
    } yield SourcedQuestion(verbIndex, question, Set.empty)
    QASRLEvaluationPrompt(sentenceId, sourceQuestions.toList)
  }

  private def getEvalPrompts(genTypeId: String): Vector[QASRLEvaluationPrompt[SentenceId]] = {
    val hitService = config.hitDataService
    val genPrompts = hitService.getAllHITInfo[QASRLGenerationPrompt[SentenceId], List[VerbQA]](genTypeId).get
    val flats: Vector[QA[SentenceId]] = flattenHitInfos[SentenceId](genPrompts)
    val evalPrompts = for {
      (sentenceId, qaPairsPerSentence) <- flats.groupBy(_.sentenceId)
    } yield createEvaluationPrompt[SentenceId](sentenceId, qaPairsPerSentence)
    evalPrompts.toVector
  }

  private def getValidationPrompts(genTypeId: String): Vector[QASRLValidationPrompt[SentenceId]] = {
    val hitService = config.hitDataService
    val genInfos = hitService.getAllHITInfo[QASRLGenerationPrompt[SentenceId], List[VerbQA]](genTypeId).get
    val valPrompts = for {
      genInfo <- genInfos
      hit = genInfo.hit
      assign <- genInfo.assignments
      qas = assign.response
    } yield QASRLValidationPrompt[SentenceId](hit.prompt, hit.hitTypeId, hit.hitId, assign.assignmentId, qas)
    valPrompts.toVector
  }

  val dataset: Map[String, Vector[String]] = {
    logger.info(s"Reading dataset from: $datasetPath")
    val reader: CSVReader = CSVReader.open(datasetPath.toString)
    // CSV format:
    // file_name, sent_id, sentence
    // 10_13ecbplus.xml,0,Report: Red Sox offer Teixeira $200 million
    val csvRecords = reader.allWithHeaders()
    (for {
      rec <- csvRecords
      id = rec("file_name") + "_" + rec("sent_id")
      sent = rec("sentence")
      tokens: Vector[String] = Tokenizer.tokenize(sent)
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

  def numEvaluationAssignmentsForPrompt(p: QASRLValidationPrompt[SentenceId]) = 3

  val allPrompts: Vector[QASRLValidationPrompt[SentenceId]] = getValidationPrompts(genTypeId)
  val experiment = new QASRLEvaluationPipeline[SentenceId](
    allPrompts,
    numEvaluationAssignmentsForPrompt)

}
