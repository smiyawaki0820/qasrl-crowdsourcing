import pandas as pd
from typing import List, Tuple
Span = Tuple[int, int]
Argument = List[Span]
ArgumentText = List[str]

SPAN_SEPARATOR = "~!~"


NO_RANGE = "NO_RANGE"
INVALID = "INVALID"

def encode_argument(arg: Argument) -> str:
    if arg[0] == NO_RANGE:
        return NO_RANGE

    return SPAN_SEPARATOR.join([encode_span(span) for span in arg])


def encode_span(span: Span):
    return "{}:{}".format(span[0], span[1])


def encode_argument_text(arg_text: ArgumentText):
    return SPAN_SEPARATOR.join(arg_text)


def argument_to_text(argument: Argument, tokens: List[str]) -> ArgumentText:
    return [span_to_text(span, tokens) for span in argument]


def span_to_text(span: Span, tokens: List[str]) -> str:
    span_start, span_end = span
    return " ".join(tokens[span_start: span_end])


def decode_span(span_str: str) -> Span:
    splits = span_str.split(":")
    return int(splits[0]), int(splits[1])


def decode_argument(arg_str: str) -> Argument:
    ranges = arg_str.split(SPAN_SEPARATOR)
    if ranges[0] == NO_RANGE:
        return ranges

    ranges = [decode_span(span_str) for span_str in ranges]
    return ranges


def decode_qasrl(qasrl_df: pd.DataFrame) -> pd.DataFrame:
    cols = set(qasrl_df.columns)
    answer_range_cols = set([col for col in cols if "answer_range" in col])
    answer_cols = set([col for col in cols  if "answer" in col]) - answer_range_cols

    for c in answer_cols:
        qasrl_df[c] = qasrl_df[c].apply(lambda a: a.split(SPAN_SEPARATOR))
    for c in answer_range_cols:
        qasrl_df[c] = qasrl_df[c].apply(decode_argument)

    return qasrl_df


def encode_qasrl(qasrl_df):
    for_csv = qasrl_df.copy()
    cols = set(qasrl_df.columns)
    answer_range_cols = set([col for col in cols if "answer_range" in col])
    answer_cols = set([col for col in cols  if "answer" in col]) - answer_range_cols

    for c in answer_range_cols:
        for_csv[c] = for_csv[c].apply(encode_argument)
    for c in answer_cols:
        for_csv[c] = for_csv[c].apply(encode_argument_text)
    return for_csv

