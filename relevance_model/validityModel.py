import xgboost as xgb
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
import argparse
import os
from preprocess.rule import REELogic
from sklearn.metrics import accuracy_score


def transform_one_hot_vector(ree, all_predicates_str):
    dimension = len(all_predicates_str)
    ree_vector = np.zeros(dimension*2)
    for predicate in ree.get_currents():
        pos = all_predicates_str.index(predicate.print_predicate_new())
        ree_vector[pos] = 1
    pos_rhs = all_predicates_str.index(ree.get_RHS().print_predicate_new())
    ree_vector[pos_rhs + dimension] = 1
    # print("dimension: ", dimension*2)
    return ree_vector


def main():
    parser = argparse.ArgumentParser(description="Learn the validity model for learn the potential validity of rules")
    parser.add_argument('-rules_file', '--rules_file', type=str, default="../datasets/airports/train_validity/rules.txt")
    parser.add_argument('-data_name', '--data_name', type=str, default="airports")
    parser.add_argument('-all_predicates', '--all_predicates', type=str, default='../datasets/airports/all_predicates.txt')
    parser.add_argument('-output_dir', '--output_dir', type=str, default='../datasets/airports/train_validity/')
    # parser.add_argument('-vary', '--vary', type=str, default="vary_supp")
    # parser.add_argument('-supp', '--supp', type=str, default="0.000001")
    # parser.add_argument('-conf', '--conf', type=str, default="0.75")
    args = parser.parse_args()
    arg_dict = args.__dict__

    if not os.path.exists(arg_dict['output_dir']):
        os.makedirs(arg_dict['output_dir'])

    # rules_file = "../datasets/" + arg_dict["data_name"] + "/train_validity/result_" + arg_dict["data_name"] + "_sample_" + arg_dict["vary"] + "_len5_supp" + str(arg_dict["supp"]) + "_conf" + str(arg_dict["conf"]) + "_top10_processor20_rel_support__div_null.txt"
    # data = pd.read_csv(rules_file, names=["rule", "label"])
    data = pd.read_csv(arg_dict["rules_file"], names=["rule", "label"])
    rules = []
    for ree_str in data["rule"].values:
        ree = REELogic()
        ree.load_X_and_e(textual_rule=(ree_str + ", supp:0, conf:0"), data_name=arg_dict["data_name"])
        rules.append(ree)
        # print(ree.print_rule())
    print("load " + str(len(rules)) + " rules")

    all_predicates_str = []
    f = open(arg_dict["all_predicates"], "r")
    for pred_str in f.readlines():
        all_predicates_str.append(pred_str.strip())
    f.close()

    X = np.array([transform_one_hot_vector(ree, all_predicates_str) for ree in rules])
    data["label"] = data["label"].str.replace("label: ", "").astype(int)
    y = data["label"].astype(int).values
    print(X, y)

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Training the model
    model = xgb.XGBClassifier(objective='multi:softmax', num_class=3, max_depth=4, learning_rate=0.1, n_estimators=100)
    model.fit(X_train, y_train)

    # Predicting probabilities
    probabilities = model.predict_proba(X_test)
    y_pred = model.predict(X_test)
    print(np.max(probabilities, axis=1))

    # Calculate the accuracy
    accuracy = accuracy_score(y_test, y_pred)
    print("Accuracy:", accuracy)
    # print(probabilities)
    # print(y_pred)

    model.save_model(arg_dict["output_dir"] + "xgboost_model.json")
    model.save_model(arg_dict["output_dir"] + "xgboost_model.bin")
    # model.save_model(arg_dict["output_dir"] + "xgboost_model_supp" + str(arg_dict["supp"]) + "_conf" + str(arg_dict["conf"]) + ".bin")


if __name__ == '__main__':
    main()
