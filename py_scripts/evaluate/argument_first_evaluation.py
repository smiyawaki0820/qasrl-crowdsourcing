import numpy
from itertools import combinations, product
from typing import Tuple, List, Set, Dict, Generator
import pandas as pd
import numpy as np
from annotations.decode_encode_answers import decode_qasrl, argument_to_text, span_to_text, NO_RANGE
from collections import defaultdict
from tqdm import tqdm


# These definitions are new formalism, and are inconsistent with rest
# of codebase, TODO - change the older codebase
Argument = Tuple[int, int]
Question = str
Role = Set[Question]
ArgumentRole = Tuple[Argument, Role]


def iou(arg1: Argument, arg2: Argument):
    joint = joint_len(arg1, arg2)
    len1 = arg1[1] - arg1[0]
    len2 = arg2[1] - arg2[0]
    union = len1 + len2 - joint
    return float(joint)/union


def joint_len(arg1, arg2):
    max_start = max(arg1[0], arg2[0])
    min_end = min(arg1[1], arg2[1])
    joint = max(min_end - max_start, 0)
    return joint


def ensure_no_overlaps(system_arguments: Set[ArgumentRole]) -> Set[ArgumentRole]:
    sys_args = sorted(system_arguments, key=lambda arg_role: arg_role[0][0] + arg_role[0][1])
    is_done = False
    while not is_done:
        conflicts = [(arg1, arg2) for arg1, arg2
                     in combinations(sys_args, r=2)
                     if joint_len(arg1[0], arg2[0])]
        if conflicts:
            conf_arg1, conf_arg2 = conflicts[0]
            print("Found conflicts!", conf_arg1, conf_arg2)
            sys_args.remove(conf_arg2)
        is_done = not conflicts
    return sorted(set(sys_args))


def find_matches(sys_args: List[Argument], grt_args: List[Argument]) \
        -> Dict[Argument, Argument]:
    matches = [(arg1, arg2, iou(arg1, arg2))
               for arg1, arg2 in product(sys_args, grt_args)]
    matches = pd.DataFrame(matches, columns=['sys_arg', 'grt_arg', 'score'])

    # get any overlapping pairs between SYS and GRT
    matches = matches[matches.score > 0].copy()

    if not matches.shape[0]:
        return dict()

    # get number of GT arguments that each SYS argument covers.
    matches['grt_arg_count'] = matches.groupby('sys_arg').grt_arg.transform(pd.Series.nunique)
    # get number of SYS arguments that each GT argument covers.
    matches['sys_arg_count'] = matches.groupby('grt_arg').sys_arg.transform(pd.Series.nunique)
    # Get only 1x1 pairs - very strict.
    # Can get just the first SYS-GT pair that maps several SYS to a single GT.
    matches.sort_values("score", ascending=False, inplace=True)
    matches = matches[matches.score > 0.3].copy()

    used_sys_args, used_grt_args = set(), set()
    passing_matches = {}
    for idx, row in matches.iterrows():
        if row.grt_arg in used_grt_args or row.sys_arg in used_sys_args:
            continue
        passing_matches[row.sys_arg] = row.grt_arg
        used_sys_args.add(row.sys_arg)
        used_grt_args.add(row.grt_arg)

    # is_one_on_one = (matches.grt_arg_count == 1) & (matches.sys_arg_count == 1)
    # matches_single_overlap = matches[is_one_on_one].copy()
    # now, select only the matches that have 0.5 in IOU score
    # passing_matches = matches_single_overlap[matches_single_overlap.score >= 0.3].copy()

    return passing_matches


def evaluate(sys_arg_roles: Set[ArgumentRole],
             grt_arg_roles: Set[ArgumentRole],
             allow_overlaps: bool):

    if not allow_overlaps:
        sys_arg_roles = ensure_no_overlaps(sys_arg_roles)

    sys_args = [a[0] for a in sys_arg_roles]
    grt_args = [a[0] for a in grt_arg_roles]
    sys_to_grt_matches = find_matches(sys_args, grt_args)

    all_arg_roles = build_all_arg_roles(sys_arg_roles,
                                        grt_arg_roles,
                                        sys_to_grt_matches)

    arg_metrics = eval_arguments(grt_arg_roles,
                                 sys_arg_roles,
                                 sys_to_grt_matches)
    role_metrics = eval_roles(grt_arg_roles,
                              sys_arg_roles,
                              sys_to_grt_matches)

    return arg_metrics, role_metrics, all_arg_roles


def eval_arguments(grt_arg_roles, sys_arg_roles, sys_to_grt_matches):
    # By argument:
    tp_arg_count = len(sys_to_grt_matches)
    fp_arg_count = len(sys_arg_roles) - tp_arg_count
    fn_arg_count = len(grt_arg_roles) - tp_arg_count
    return tp_arg_count, fp_arg_count, fn_arg_count


def build_role_to_arg(arg_roles: List[ArgumentRole]):
    role2arg = defaultdict(list)
    for arg, role in arg_roles:
        role2arg[role].append(arg)
    return role2arg


def is_full_arg_match(grt_args: List[Argument],
                      sys_args: List[Argument],
                      sys_to_grt: Dict[Argument, Argument]):

    if len(grt_args) != len(sys_args):
        return False
    # how many GRT args are aligned with SYS args?
    # Assume matches does not contain duplicate alignments.
    grt_arg_set = set(grt_args)
    for sys_arg in sys_args:
        matched_grt_arg = sys_to_grt.get(sys_arg)
        if matched_grt_arg not in grt_arg_set:
            return False
        else:
            # so we would not match an item twice, jsut in case
            grt_arg_set.remove(matched_grt_arg)

    # All arguments in both roles were aligned
    return True


def eval_roles(grt_arg_roles,
               sys_arg_roles,
               sys_to_grt: Dict[Argument, Argument]):

    grt_role2arg = build_role_to_arg(grt_arg_roles)
    sys_role2arg = build_role_to_arg(sys_arg_roles)

    sys_unused_roles = set(sys_role2arg.keys())
    role_matches = []
    for grt_role, grt_args in grt_role2arg.items():
        has_matched = False
        for sys_role in sys_unused_roles:
            sys_args = sys_role2arg[sys_role]
            has_matched = is_full_arg_match(grt_args, sys_args, sys_to_grt)
            if has_matched:
                role_matches.append((grt_role, sys_role))
                break
        if has_matched:
            last_sys_match = role_matches[-1][1]
            sys_unused_roles.remove(last_sys_match)

    tp_role = len(role_matches)
    fp_role = len(sys_role2arg) - tp_role
    fn_role = len(grt_role2arg) - tp_role
    return tp_role, fp_role, fn_role


def build_all_arg_roles(sys_arg_roles: Set[ArgumentRole],
                        grt_arg_roles: Set[ArgumentRole],
                        sys_to_grt_matches: Dict[Argument, Argument]):
    grt_arg_roles = pd.DataFrame(list(grt_arg_roles), columns=["grt_arg", "grt_role"])
    sys_arg_roles = pd.DataFrame(list(sys_arg_roles), columns=["sys_arg", "sys_role"])
    # Dictionary mapping with None values
    sys_arg_roles['grt_arg'] = sys_arg_roles.sys_arg.apply(sys_to_grt_matches.get)
    all_arg_roles = pd.merge(sys_arg_roles, grt_arg_roles, on="grt_arg", how="outer")
    all_arg_roles.grt_arg.fillna(NO_RANGE, inplace=True)
    all_arg_roles.sys_arg.fillna(NO_RANGE, inplace=True)

    return all_arg_roles


def filter_ids(df, row):
    return (df.qasrl_id == row.qasrl_id) & (df.verb_idx == row.verb_idx)


def prec(metrics: np.array):
    tp, fp, _ = metrics
    return float(tp)/(tp + fp)


def recall(metrics: np.array):
    tp, _, fn = metrics
    return float(tp)/(tp + fn)


def f1(metrics):
    p, r = prec(metrics), recall(metrics)
    return 2*p*r/(p+r)


def fill_answer(arg: Argument, tokens: List[str]):
    if arg == NO_RANGE:
        return NO_RANGE
    return " ".join(tokens[arg[0]: arg[1]])


def eval_datasets(sys_df, grt_df, sent_map, allow_overlaps: bool) -> Tuple[np.array, pd.DataFrame]:
    arg_counts = np.zeros(3, dtype=np.float32)
    role_counts = np.zeros(3, dtype=np.float32)
    all_matchings = []
    for key, sys_arg_roles, grt_arg_roles in yield_paired_predicates(sys_df, grt_df):
        qasrl_id, verb_idx = key
        tokens = sent_map[qasrl_id]
        local_arg_counts, local_role_counts, all_args = evaluate(sys_arg_roles, grt_arg_roles, allow_overlaps)
        arg_counts += np.array(local_arg_counts)
        role_counts += np.array(local_role_counts)
        all_args['qasrl_id'] = qasrl_id
        all_args['verb_idx'] = verb_idx
        all_args['grt_arg_text'] = all_args.grt_arg.apply(fill_answer, tokens=tokens)
        all_args['sys_arg_text'] = all_args.sys_arg.apply(fill_answer, tokens=tokens)
        all_matchings.append(all_args)

    all_matchings = pd.concat(all_matchings)
    all_matchings = all_matchings[['grt_arg_text', 'sys_arg_text',
                                   'grt_role', 'sys_role',
                                   'grt_arg', 'sys_arg',
                                   'qasrl_id', 'verb_idx']]

    # all_matchings.sys_arg = all_matchings.sys_arg.apply(encode_span)
    # all_matchings.grt_arg = all_matchings.grt_arg.apply(encode_span)
    return arg_counts, role_counts, all_matchings


def get_metrics(err_counts):
    return prec(err_counts), recall(err_counts), f1(err_counts)


def main():
    sent_df = pd.read_csv("../mult_generation/wikinews/wikinews.dev.data.csv")
    sent_map = dict(zip(sent_df.qasrl_id, sent_df.tokens.apply(str.split)))

    # sys_df = decode_qasrl(pd.read_csv("../nrl/wikinews.dev.data.pred.0_5.csv"))
    # sys_df = decode_qasrl(pd.read_csv("../nrl/wikinews.dev.data.pred.csv"))
    sys_df = decode_qasrl(pd.read_csv("../qasrl-v2/dense/dev.consolidated.max_inv_3.csv"))
    grt_df = decode_qasrl(pd.read_csv("../mult_generation/wikinews/wikinews.dev.manual_annotated.csv"))
    print(sum(sys_df.answer_range.apply(len).tolist()))
    arg, role, _ = eval_datasets(sys_df, grt_df, sent_map, allow_overlaps=False)

    print("ARGUMENT: Prec/Recall ", get_metrics(arg))
    print("ROLE: Prec/Recall ", get_metrics(role))


def main3():
    from mult_generation.inter_annotator import get_group_path
    base_path = "../mult_generation/wikinews/wikinews.dev.qasrl.mult_gen.csv"

    grt_df = decode_qasrl(pd.read_csv("../mult_generation/wikinews/wikinews.dev.manual_annotated.csv"))
    sent_df = pd.read_csv("../mult_generation/wikinews/wikinews.dev.data.csv")
    sent_df.tokens = sent_df.tokens.apply(str.split)
    sent_map = dict(zip(sent_df.qasrl_id, sent_df.tokens))
    arg_precs, arg_recalls = [], []
    role_precs, role_recalls = [], []
    for group in combinations(numpy.arange(6), r=3):
    # for group in numpy.arange(1,7):
        group_path = get_group_path(base_path, sorted(group), clustered=True)
        # "../mult_generation/wikinews/splits/wikinews.dev.qasrl.mult_gen.{}.clustered.csv"
        sys_df = decode_qasrl(pd.read_csv(group_path))
        predicate_ids = grt_df[['qasrl_id', 'verb_idx']].drop_duplicates()
        all_arg_metrics = np.zeros(3)
        all_role_metrics = np.zeros(3)
        for idx, row in predicate_ids.iterrows():
            # print(row.ecb_id, row.verb_idx)
            sys_arg_roles = sys_df[filter_ids(sys_df, row)].copy()
            grt_arg_roles = grt_df[filter_ids(grt_df, row)].copy()

            sys_arg_roles = set(yield_argument_roles(sys_arg_roles))
            grt_arg_roles = set(yield_argument_roles(grt_arg_roles))

            arg_metrics, role_metrics, _ = evaluate(sys_arg_roles, grt_arg_roles)
            all_arg_metrics += np.array(arg_metrics)
            all_role_metrics += np.array(role_metrics)

        arg_precs.append(prec(all_arg_metrics))
        arg_recalls.append(recall(all_arg_metrics))

        role_precs.append(prec(all_role_metrics))
        role_recalls.append(recall(all_role_metrics))

        print("ARGUMENT, Precision: {:2.2f}% \t Recall: {:2.2f}%".format(arg_precs[-1]*100, arg_recalls[-1]*100))
        print("ROLE, Precision: {:2.2f}% \t Recall: {:2.2f}%".format(role_precs[-1]*100, role_recalls[-1]*100))

    arg_precs, arg_recalls = np.array(arg_precs), np.array(arg_recalls)
    role_precs, role_recalls = np.array(role_precs), np.array(role_recalls)
    print()
    print("MEAN METRICS FOR ALL SPLITS")

    print("ARGUMENT, Precision: {} +- {}".format(arg_precs.mean(), arg_precs.std()))
    print("ARGUMENT, Recall: {} +- {}".format(arg_recalls.mean(), arg_recalls.std()))
    print("ROLE, Precision: {} +- {}".format( role_precs.mean(), role_precs.std()))
    print("ROLE, Recall: {} +- {}".format(role_recalls.mean(), role_recalls.std()))


def yield_paired_predicates(sys_df, grt_df):
    predicate_ids = grt_df[['qasrl_id', 'verb_idx']].drop_duplicates()
    for idx, row in predicate_ids.iterrows():
        sys_arg_roles = sys_df[filter_ids(sys_df, row)].copy()
        grt_arg_roles = grt_df[filter_ids(grt_df, row)].copy()
        sys_arg_roles = list(yield_argument_roles(sys_arg_roles))
        grt_arg_roles = list(yield_argument_roles(grt_arg_roles))
        yield (row.qasrl_id, row.verb_idx), set(sys_arg_roles), set(grt_arg_roles)


def yield_argument_roles(sys_arg_roles) -> Generator[ArgumentRole, None, None]:
    sys_args = sys_arg_roles.answer_range.tolist()
    sys_roles = sys_arg_roles.question.tolist()
    for answer_ranges, question in zip(sys_args, sys_roles):
        for answer_range in answer_ranges:
            yield answer_range, question


if __name__ == "__main__":
    main()
