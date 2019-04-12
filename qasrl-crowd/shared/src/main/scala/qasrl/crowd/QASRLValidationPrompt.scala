package qasrl.crowd

case class QASRLValidationPrompt[SID](
                                       genPrompt: QASRLGenerationPrompt[SID],
                                       sourceHITTypeId: String,
                                       sourceHITId: String,
                                       sourceAssignmentId: String,
                                       qaPairs: List[VerbQA]
                                     ) {
  def id = genPrompt.id
}

case class QASRLArbitrationPrompt[SID](
                                        genPrompt: QASRLGenerationPrompt[SID],
                                        qaPairs: List[(String, VerbQA)]
                                      ) {
  def id = genPrompt.id
}
