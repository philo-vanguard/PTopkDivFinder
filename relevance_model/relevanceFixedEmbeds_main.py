import argparse
import logging
import sys

import numpy as np
from sklearn.model_selection import train_test_split
from relevanceFixedEmbeds import *
from utils import *
import pandas as pd
import pickle as plk
import os
import tensorflow as tf
from transformers import BertTokenizer, BertModel
import torch


def main():
    parser = argparse.ArgumentParser(description="Learn the relevance model with fixed embeddings")
    parser.add_argument('-lr', '--lr', type=float, default=0.01)
    parser.add_argument('-lr_pre_train', '--lr_pre_train', type=float, default=0.001)
    parser.add_argument('-hidden_size', '--hidden_size', type=int, default=100)
    parser.add_argument('-rees_embed_dim', '--rees_embed_dim', type=int, default=100)
    parser.add_argument('-epochs', '--epochs', type=int, default=400)
    parser.add_argument('-epochs_pre_train', '--epochs_pre_train', type=int, default=100)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=128)
    parser.add_argument('-pretrained_matrix_file', '--pretrained_matrix_file', type=str, default='../datasets/airports/predicateEmbedds.csv')
    parser.add_argument('-rules_file', '--rules_file', type=str, default='../datasets/airports/rules.txt')
    parser.add_argument('-train_file', '--train_file', type=str, default='../datasets/airports/train_0/train.csv')
    parser.add_argument('-test_file', '--test_file', type=str, default='../datasets/airports/train_0/test.csv')
    parser.add_argument('-output_model_dir', '--output_model_dir', type=str, default='../datasets/airports/train_0/model/')
    parser.add_argument('-all_predicates_file', '--all_predicates_file', type=str, default='../datasets/airports/all_predicates.txt')
    parser.add_argument('-modelOpt', '--modelOpt', type=str, default="/Users/hanzy/OneDrive-Local/Publications/codes/LMs/distilbert-base-uncased")
    parser.add_argument('-margin', '--margin', type=float, default=0.7)
    parser.add_argument('-times', '--times', type=str, default="1")
    args = parser.parse_args()
    arg_dict = args.__dict__

    if not os.path.exists(arg_dict['output_model_dir']):
        os.makedirs(arg_dict['output_model_dir'])

    # load data
    train_data = pd.read_csv(arg_dict['train_file'])
    test_data = pd.read_csv(arg_dict['test_file'])
    rule_data_header = ["rule", "support", "confidence"]
    rees_data = pd.read_csv(arg_dict['rules_file'], names=rule_data_header)["rule"].values

    # split train and valid
    train_data, valid_data = train_test_split(train_data, train_size=0.9, random_state=42)
    train_pair_ids, train_labels = train_data[['left_id', 'right_id']].values, train_data['label'].values
    valid_pair_ids, valid_labels = valid_data[['left_id', 'right_id']].values, valid_data['label'].values
    test_pair_ids, test_labels = test_data[['left_id', 'right_id']].values, test_data['label'].values
    train_labels_vec = convert_to_vector(train_labels)
    valid_labels_vec = convert_to_vector(valid_labels)
    test_labels_vec = convert_to_vector(test_labels)
    train_labels = convert_to_onehot(train_labels)
    valid_labels = convert_to_onehot(valid_labels)
    test_labels = convert_to_onehot(test_labels)

    # load all predicates into file
    all_predicates_str = []
    f = open(arg_dict["all_predicates_file"], "r")
    for pred_str in f.readlines():
        all_predicates_str.append(pred_str.strip())
    f.close()
    if "PAD" not in all_predicates_str:
        all_predicates_str.insert(0, "PAD")

    all_predicate_size = len(all_predicates_str)  # includes 'PAD'
    print("all_predicate_size len: ", all_predicate_size)

    if os.path.exists(arg_dict["pretrained_matrix_file"]):
        # load pretrained matrix, i.e., the initial embeddings of predicates
        predicate_initial_embedds = pd.read_csv(arg_dict['pretrained_matrix_file'], names=[str(e) for e in range(768)]).values
        predicate_embedding_size = predicate_initial_embedds.shape[1]
        print("predicate_initial_embedds shape:", predicate_initial_embedds.shape)

    else:
        # predicate embeddings initialized by Bert
        predicate_embedding_size = 768
        tokenizer = BertTokenizer.from_pretrained(arg_dict["modelOpt"])
        bert_model = BertModel.from_pretrained(arg_dict["modelOpt"])
        encoded_input = tokenizer(all_predicates_str, return_tensors='pt', padding=True, truncation=True)
        with torch.no_grad():
            output = bert_model(**encoded_input)
        predicate_initial_embedds = output.last_hidden_state.mean(dim=1)
        print(predicate_initial_embedds.shape[0])
        print("predicate_embedding_size:", predicate_embedding_size)

    # process rules into bit vectors
    rees_lhs, rees_rhs = processAllRulesByPredicates(rees_data, all_predicates_str)
    print('The first 10 data')
    print(rees_lhs[:5])
    print(rees_rhs[:5])

    # predicate_initial_embedds = np.random.rand(109, 768)

    # model
    model = RelevanceEmbeds(all_predicate_size, predicate_embedding_size, arg_dict['hidden_size'],
                            arg_dict['rees_embed_dim'], MAX_LHS_PREDICATES,
                            MAX_RHS_PREDICATES, arg_dict['lr'], arg_dict['epochs'], arg_dict['batch_size'],
                            predicate_initial_embedds,
                            arg_dict["margin"],
                            arg_dict["epochs_pre_train"], arg_dict["lr_pre_train"])

    if not os.path.exists(arg_dict["pretrained_matrix_file"]):
        predEmbeds_intial = np.squeeze(model.getAllPredicateEmbeddings(), axis=0)
        print("predicate embeddings intially:\n", predEmbeds_intial)
        print(predEmbeds_intial.shape)

        # pre-train to obtain the initial predicate embeddings using contrastive loss function
        model.pre_training_train(rees_lhs, rees_rhs, train_pair_ids, train_labels_vec, valid_pair_ids, valid_labels_vec)
        model.pre_training_evaluate(rees_lhs, rees_rhs, test_pair_ids, test_labels_vec)

        predEmbeds_pre_train = np.squeeze(model.getAllPredicateEmbeddings(), axis=0)
        print("After pre-train, predicate embeddings:\n", predEmbeds_pre_train)
        print(predEmbeds_intial.shape)
        pd.DataFrame(predEmbeds_pre_train).to_csv(arg_dict["pretrained_matrix_file"], index=False, header=False)
        sys.exit()

    # train
    model.train(rees_lhs, rees_rhs, train_pair_ids, train_labels, valid_pair_ids, valid_labels)
    # evaluate
    test_log = model.evaluate(rees_lhs, rees_rhs, test_pair_ids, test_labels)
    f = open(arg_dict['output_model_dir'] + "acc-" + arg_dict["times"] + ".txt", "w")
    f.write(test_log)
    f.close()
    # save the rule interestingness model in TensorFlow type
    model.saveModel(arg_dict['output_model_dir'] + "model")
    # save the rule interestingness model in TXT type
    model.saveModelToText(arg_dict['output_model_dir'] + "model.txt")


if __name__ == '__main__':
    main()

