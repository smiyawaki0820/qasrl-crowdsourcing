package qasrl.labeling

import qasrl._
import qasrl.util._
import qasrl.util.implicits._

import cats.Id
import cats.implicits._
import cats.arrow.Arrow
import cats.data.NonEmptyList

import nlpdata.datasets.wiktionary.InflectedForms
import nlpdata.util.LowerCaseStrings._

// for accordance with original QA-SRL format
case class SlotBasedLabel[A](
  wh: LowerCaseString,
  aux: Option[LowerCaseString],
  subj: Option[LowerCaseString],
  verbPrefix: List[LowerCaseString],
  verb: A,
  obj: Option[LowerCaseString],
  prep: Option[LowerCaseString],
  obj2: Option[LowerCaseString]) {

  def slotStrings(renderVerb: A => LowerCaseString) = List(
    wh,
    aux.getOrElse("_".lowerCase),
    subj.getOrElse("_".lowerCase),
    (verbPrefix.mkString(" ") + " " + renderVerb(verb).toString).lowerCase,
    obj.getOrElse("_".lowerCase),
    prep.getOrElse("_".lowerCase),
    obj2.getOrElse("_".lowerCase)
  )

  def renderWithSeparator(renderVerb: A => LowerCaseString, sep: String) =
    slotStrings(renderVerb).mkString(sep)
}

object SlotBasedLabel {

  val mainAuxVerbs = {
    val negContractibleAuxes = Set(
      "has", "had",
      "might", "would", "should",
      "does", "did",
      "is", "was"
    )
    val allAuxes = negContractibleAuxes ++
      negContractibleAuxes.map(_ + "n't") ++
      Set(
        "can", "will",
        "can't", "won't"
      )
    allAuxes.map(_.lowerCase)
  }

  val getSlotsForQuestion = QuestionLabelMapper(
    (sentenceTokens: Vector[String],
     verbInflectedForms: InflectedForms,
     questions: List[String]
    ) => {
      questions.flatMap { question =>
        val stateMachine = new TemplateStateMachine(sentenceTokens, verbInflectedForms)
        val template = new QuestionProcessor(stateMachine)
        val resOpt = template.processStringFully(question) match {
          case Left(QuestionProcessor.AggregatedInvalidState(_, _)) => None
          case Right(goodStates) => goodStates.toList.collect {
            case QuestionProcessor.CompleteState(_, frame, answerSlot) =>
              val wh = {
                val whStr = answerSlot match {
                  case Subj => if(frame.args.get(Subj).get.isAnimate) "who" else "what"
                  case Obj => if(frame.args.get(Obj).get.isAnimate) "who" else "what"
                  case Obj2 => frame.args.get(Obj2).get match {
                    case Noun(isAnimate) => if(isAnimate) "who" else "what"
                    case Prep(_, Some(Noun(isAnimate))) => if(isAnimate) "who" else "what"
                    case Locative => "where"
                    case _ => "what" // extra case for objless prep; shouldn't happen
                  }
                  case Adv(wh) => wh.toString
                }
                whStr.lowerCase
              }
              val subj = {
                if(answerSlot == Subj) None
                else frame.args.get(Subj).map { case Noun(isAnimate) =>
                  (if(isAnimate) "someone" else "something").lowerCase
                }
              }
              val verbStack = if(subj.isEmpty) {
                frame.getVerbStack.map(_.lowerCase)
              } else {
                frame.splitVerbStackIfNecessary(frame.getVerbStack).map(_.lowerCase)
              }
              val (aux, verbTokens) = NonEmptyList.fromList(verbStack.tail)
                .filter(_ => mainAuxVerbs.contains(verbStack.head)) match {
                case None => None -> verbStack
                case Some(tail) => Some(verbStack.head) -> tail
              }
              val verbPrefix = verbTokens.init
              val verb = verbTokens.last

              val obj = {
                if(answerSlot == Obj) None
                else frame.args.get(Obj).map { case Noun(isAnimate) =>
                  (if(isAnimate) "someone" else "something").lowerCase
                }
              }

              val (prep, obj2) = {
                frame.args.get(Obj2).fold(Option.empty[LowerCaseString] -> Option.empty[LowerCaseString]) { arg =>
                  if(answerSlot == Obj2) arg match {
                    case Noun(isAnimate) => None -> None
                    case Prep(preposition, _) =>
                      val vec = preposition.split(" ").toVector
                      if(vec.last == "do" || vec.last == "doing") {
                        Some(vec.init.mkString(" ").lowerCase) -> Some(vec.last.lowerCase)
                      } else Some(preposition) -> None
                    case Locative => None -> None
                  } else arg match {
                    case Noun(isAnimate) => None -> Some((if(isAnimate) "someone" else "something").lowerCase)
                    case Prep(preposition, Some(Noun(isAnimate))) =>
                      val vec = preposition.split(" ").toVector.init
                      if(vec.size > 0 && (vec.last == "do" || vec.last == "doing")) {
                        Some(vec.init.mkString(" ").lowerCase) -> Some((vec.last + " something").lowerCase)
                      } else Some(preposition) -> Some((if(isAnimate) "someone" else "something").lowerCase)
                    case Prep(preposition, None) =>
                      Some(preposition) -> None
                    case Locative => None -> Some("somewhere".lowerCase)
                  }
                }
              }
              SlotBasedLabel(wh, aux, subj, verbPrefix, verb, obj, prep, obj2)
          }.toSet.headOption
        }
        resOpt.map(question -> _)
      }.toMap: Map[String, SlotBasedLabel[LowerCaseString]]
    }
  )

  val getVerbTenseAbstractedSlotsForQuestion = (
    Arrow[QuestionLabelMapper].id &&& getSlotsForQuestion
  ) >>> QuestionLabelMapper.liftOptionalWithContext(
    (sentenceTokens: Vector[String],
     verbInflectedForms: InflectedForms,
     pair: (String, SlotBasedLabel[LowerCaseString])
    ) => {
      val stateMachine = new TemplateStateMachine(sentenceTokens, verbInflectedForms)
      val template = new QuestionProcessor(stateMachine)
      val (question, rawSlots) = pair
      val frameWithAnswerSlotOpt = template.processStringFully(question) match {
        case Left(QuestionProcessor.AggregatedInvalidState(_, _)) => None
        case Right(goodStates) => goodStates.toList.collect {
          case QuestionProcessor.CompleteState(_, frame, answerSlot) => frame -> answerSlot
        }.headOption
      }
      frameWithAnswerSlotOpt.map {
        case (firstFrame, answerSlot) => rawSlots.copy(
          verb = firstFrame.getVerbConjugation(firstFrame.args.get(Subj).isEmpty || answerSlot != Subj)
        )
      }
    }
  )
}