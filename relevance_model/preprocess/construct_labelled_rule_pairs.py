import pandas as pd
import argparse
import random
import numpy as np
import os
import difflib
from rule import REELogic
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
from snorkel.labeling import labeling_function
from snorkel.labeling import PandasLFApplier
from snorkel.labeling.model import LabelModel

# global variable
all_rules = None
data_df = None
words_ids, words_embed = None, None
threshold_low, threshold_high = None, None

# labels
SIMILAR = 1
DISSIMILAR = 0
ABSTAIN = -1

all_similarity_scores = []  # all similarity scores by each label function


def readEmbedFile(embedFile):
    words = {}
    input = open(embedFile, 'r', encoding='utf-8')
    lines = []
    for line in input:
        if "tt__" in line or "cid__" in line:
            lines.append(line)
    input.close()
    nwords = len(lines) - 1
    splits = lines[1].split(' ')
    dim = len(splits) - 1
    embeddings = []

    for lineId in range(len(lines)):
        splits = lines[lineId].split(' ')
        words[splits[0].strip().replace("##", " ")] = lineId
        emb = [splits[i].strip() for i in range(1, dim+1)]
        embeddings.append(emb)
    embeddings = np.array(embeddings, dtype=np.float)
    return words, embeddings


@labeling_function()
def lf_overlap_predicates(x):
    overlap_ratio = all_rules[x.left_id].cal_predicates_overlap(all_rules[x.right_id])
    all_similarity_scores.append(overlap_ratio)
    if overlap_ratio > threshold_high:
        return SIMILAR
    elif overlap_ratio < threshold_low:
        return DISSIMILAR
    else:
        return ABSTAIN
    # if overlap_ratio > 0.5:
    #     return SIMILAR
    # else:
    #     return DISSIMILAR


@labeling_function()
def lf_overlap_tuples(x):
    satisfied_tuple_t0 = all_rules[x.left_id].get_satisfied_tuple_ids(data_df=data_df)
    satisfied_tuple_t1 = all_rules[x.right_id].get_satisfied_tuple_ids(data_df=data_df)
    if satisfied_tuple_t0 is None or satisfied_tuple_t1 is None:
        all_similarity_scores.append(0.0)
        return DISSIMILAR

    unionset = list(set(satisfied_tuple_t0).union(set(satisfied_tuple_t1)))
    intersection = list(set(satisfied_tuple_t0) & set(satisfied_tuple_t1))
    overlap_ratio = len(intersection) * 1.0 / len(unionset)

    all_similarity_scores.append(overlap_ratio)
    if overlap_ratio > threshold_high:
        return SIMILAR
    elif overlap_ratio < threshold_low:
        return DISSIMILAR
    else:
        return ABSTAIN
    # if overlap_ratio > 0.5:
    #     return SIMILAR
    # else:
    #     return DISSIMILAR


def get_word_ids(rule):
    word_ids_list = []
    for predicate in rule.get_currents():
        if predicate.get_type() == "constant":
            word_ids_list.append(words_ids["cid__" + predicate.get_attr1()])
            for constant in predicate.get_constant().split(" "):
                if ("tt__" + constant) not in words_ids.keys():
                    continue
                word_ids_list.append(words_ids["tt__" + constant])
        elif predicate.get_type() == "non-constant":
            word_ids_list.append(words_ids["cid__" + predicate.get_attr1()])
            word_ids_list.append(words_ids["cid__" + predicate.get_attr2()])
    rhs = rule.get_RHS()
    if rhs.get_type() == "constant":
        word_ids_list.append(words_ids["cid__" + rhs.get_attr1()])
        for constant in rhs.get_constant().split(" "):
            if ("tt__" + constant) not in words_ids.keys():
                continue
            word_ids_list.append(words_ids["tt__" + constant])
    elif rhs.get_type() == "non-constant":
        word_ids_list.append(words_ids["cid__" + rhs.get_attr1()])
        word_ids_list.append(words_ids["cid__" + rhs.get_attr2()])
    return word_ids_list


# transform rules to embedding, then compute the similarity
@labeling_function()
def lf_similarity_rule(x):
    # get all word ids in rule
    ids_list_rule_0 = get_word_ids(all_rules[x.left_id])
    ids_list_rule_1 = get_word_ids(all_rules[x.right_id])

    # get rule embeddings
    rule_embed_0 = words_embed[ids_list_rule_0[0]]
    for i in range(1, len(ids_list_rule_0)):
        rule_embed_0 = rule_embed_0 + words_embed[ids_list_rule_0[i]]

    rule_embed_1 = words_embed[ids_list_rule_1[0]]
    for i in range(1, len(ids_list_rule_1)):
        rule_embed_1 = rule_embed_1 + words_embed[ids_list_rule_1[i]]

    # use consine to compute the similarity
    similarity = cosine_similarity(np.expand_dims(rule_embed_0, axis=0), np.expand_dims(rule_embed_1, axis=0))[0][0]
    all_similarity_scores.append(similarity)
    if similarity > threshold_high:
        return SIMILAR
    elif similarity < threshold_low:
        return DISSIMILAR
    else:
        return ABSTAIN
    # if similarity > 0.5:
    #     return SIMILAR
    # else:
    #     return DISSIMILAR    


@labeling_function()
def lf_similarity_semantics(x):
    sentence_rule0 = all_rules[x.left_id].print_rule()
    sentence_rule1 = all_rules[x.right_id].print_rule()

    # similarity = difflib.SequenceMatcher(None, sentence_rule0, sentence_rule1).ratio()
    model = SentenceTransformer('all-MiniLM-L6-v2')
    r0_embeddings = model.encode(sentence_rule0)
    r1_embeddings = model.encode(sentence_rule1)
    similarity = cosine_similarity(r0_embeddings.reshape(1, -1), r1_embeddings.reshape(1, -1))[0][0]

    all_similarity_scores.append(similarity)
    if similarity > threshold_high:
        return SIMILAR
    elif similarity < threshold_low:
        return DISSIMILAR
    else:
        return ABSTAIN
    # if similarity > 0.5:
    #     return SIMILAR
    # else:
    #     return DISSIMILAR


@labeling_function()
def lf_same_rhs(x):
    if all_rules[x.left_id].get_RHS().print_predicate() == all_rules[x.right_id].get_RHS().print_predicate():
        all_similarity_scores.append(1)
        return SIMILAR
    else:
        all_similarity_scores.append(-1)
        return ABSTAIN


def load_rules(path):
    f = open(path, "r")  # , encoding='utf-8'
    lines = f.readlines()
    f.close()

    rees = []
    for line in lines:
        if "->" not in line:
            continue
        ree = REELogic()
        ree.load_X_and_e(line)
        rees.append(ree)
        print(ree.print_rule())

    print("finish loading, ", len(rees), " rules")
    return rees


def construct_rule_pairs(all_rules, num_pairs):
    # random.seed(1234567)
    size = len(all_rules)
    rule_pairs = set()
    while len(rule_pairs) < num_pairs:
        id1 = random.randint(0, size-1)
        id2 = random.randint(0, size-1)
        while id1 == id2:
            id2 = random.randint(0, size-1)
        if id1 <= id2:
            rule_pairs.add((id1, id2))
        else:
            rule_pairs.add((id2, id1))

    rule_pairs = pd.DataFrame(rule_pairs)
    rule_pairs.columns = ["left_id", "right_id"]

    return rule_pairs


def save_rule_pairs(all_rules, rule_pairs_df, output_path):
    f = open(output_path, "w")
    for index, row in rule_pairs_df.iterrows():
        f.writelines(str(index) + ":\n")
        f.writelines("rid " + str(row["left_id"]) + ": " + all_rules[row["left_id"]].print_rule() + "\n")
        f.writelines("rid " + str(row["right_id"]) + ": " + all_rules[row["right_id"]].print_rule() + "\n\n")
    f.close()


def main():
    parser = argparse.ArgumentParser(description="Given rule pairs, generate labels to represent whether the two rules are correlated.")
    parser.add_argument('-data_path', '--data_path', type=str, default="../../datasets/airports/airports_sample.csv")
    parser.add_argument('-rule_path', '--rule_path', type=str, default="../../datasets/airports/rules.txt")  # obtained by rule discovery
    parser.add_argument('-words_embed_path', '--words_embed_path', type=str, default="../../datasets/airports/airports_embedding_filtered.emb")  # obtained by EmbDI
    parser.add_argument('-labelled_data_dir', '--labelled_data_dir', type=str, default="../../datasets/airports/train")  # the dir of output labelled data and other intermediate results
    parser.add_argument('-rule_pairs_num', '--rule_pairs_num', type=int, default=3000)  # the number of whole labelled rule pairs
    parser.add_argument('-threshold_low', '--threshold_low', type=float, default=0.5)
    parser.add_argument('-threshold_high', '--threshold_high', type=float, default=0.8)
    parser.add_argument('-train_ratio', '--train_ratio', type=float, default=0.8)
    parser.add_argument('-times', '--times', type=int, default=1)
    args = parser.parse_args()
    arg_dict = args.__dict__

    output_labelled_data_dir = arg_dict["labelled_data_dir"] + "_" + str(arg_dict["threshold_low"]) + "_" + str(arg_dict["threshold_high"]) + "/"
    if not os.path.exists(output_labelled_data_dir):
        os.makedirs(output_labelled_data_dir)

    global all_rules, data_df, words_ids, words_embed, threshold_low, threshold_high
    data_df = pd.read_csv(arg_dict["data_path"])
    all_rules = load_rules(arg_dict["rule_path"])
    rule_pairs_df = construct_rule_pairs(all_rules, arg_dict["rule_pairs_num"])  # left_id, right_id
    save_rule_pairs(all_rules, rule_pairs_df, output_labelled_data_dir + "rules_pairs.txt")
    words_ids, words_embed = readEmbedFile(arg_dict["words_embed_path"])
    threshold_low, threshold_high = arg_dict["threshold_low"], arg_dict["threshold_high"]

    # save rules with concise format
    f = open(output_labelled_data_dir + "rules_consise.txt", "w")
    f.writelines("rule,supp,conf\n")
    for rule in all_rules:
        f.writelines(rule.print_rule() + ", supp: " + str(int(rule.get_support())) + ", conf: " + str(rule.get_confidence()) + "\n")
    f.close()

    lfs = [
        lf_overlap_predicates,
        lf_overlap_tuples,
        lf_similarity_rule,
        lf_similarity_semantics,
        lf_same_rhs,
    ]

    applier = PandasLFApplier(lfs)
    L_train = applier.apply(rule_pairs_df)
    np.savetxt(output_labelled_data_dir + "labelled_scores.txt", L_train, fmt="%d")

    label_model = LabelModel(cardinality=2, verbose=True)
    label_model.fit(L_train, seed=1234567, lr=0.001, log_freq=10, n_epochs=100)

    rule_pairs_df["label"] = label_model.predict(L_train)
    rule_pairs_df.to_csv(output_labelled_data_dir + "labelled_data.csv", index=False)

    train_size = int(rule_pairs_df.shape[0] * arg_dict["train_ratio"])
    rule_pairs_df.loc[:train_size, :].to_csv(output_labelled_data_dir + "train.csv", index=False)
    rule_pairs_df.loc[train_size:, :].to_csv(output_labelled_data_dir + "test.csv", index=False)

    global all_similarity_scores
    all_similarity_scores = np.array(all_similarity_scores)
    all_similarity_scores = all_similarity_scores.reshape(arg_dict["rule_pairs_num"], len(lfs))
    # np.savetxt(output_labelled_data_dir + "temp_similarity.txt", all_similarity_scores, fmt="%0.2f")
    all_similarity_scores = pd.DataFrame(all_similarity_scores, columns=["overlap_predicates", "overlap_tuples", "similarity_EmbDI", "similarity_semantics", "same_rhs"])
    all_similarity_scores.to_csv(output_labelled_data_dir + "labelled_similarity.txt", index=False, float_format="%.2f", sep=",")


if __name__ == '__main__':
    main()
