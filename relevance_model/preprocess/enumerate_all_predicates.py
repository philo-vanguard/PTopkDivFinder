import pandas as pd
import numpy as np
import argparse
import operator
from rule import Predicate


operators_dict = {'==': operator.eq, '<>': operator.ne, '>': operator.gt, '<': operator.lt, '>=': operator.ge, '<=': operator.le}
comparable_ops = [">", "<", ">=", "<="]


def pick_constants(tuples, all_attrs, constant_ratio_low, constant_ratio_high):
    total_size = tuples.shape[0]
    size_lower_bound = total_size * 1.0 * constant_ratio_low
    size_upper_bound = total_size * 1.0 * constant_ratio_high
    constants_filtered = {}
    for attr in all_attrs:
        value2times = tuples[attr].value_counts()
        values = value2times[(value2times >= size_lower_bound) & (value2times <= size_upper_bound)].index.values
        constants_filtered[attr] = list(values)
    return constants_filtered


def enumerate_predicates(predicate_operators, tid_num, all_attrs, comparable_attrs, constants_filtered, data_name):
    all_predicates = []
    for t_i in range(tid_num):
        for attr in all_attrs:
            for op in predicate_operators:
                if attr not in comparable_attrs and op in comparable_ops:
                    continue
                # (1) enumerate constant predicate
                for constant in constants_filtered[attr]:
                    pre = Predicate(data_name)
                    pre.assign_info(t_i, None, attr, "", constant, op, "constant")
                    all_predicates.append(pre)
                # (2) enumerate non-constant predicate
                for t_j in range(t_i+1, tid_num):
                    pre = Predicate(data_name)
                    pre.assign_info(t_i, t_j, attr, attr, "", op, "non-constant")
                    all_predicates.append(pre)

    return all_predicates


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Given a relational data(.csv), enumerate all predicates.")
    parser.add_argument("-data_dir", "--data_dir", type=str, default="../../datasets/airports/")
    parser.add_argument("-data_name", "--data_name", type=str, default="airports")
    parser.add_argument("-tuple_variable_num", "--tuple_variable_num", type=int, default=2)
    parser.add_argument("-predicate_operators", "--predicate_operators", type=str, default="==")  # "==,<>,>,>=,<,<="
    parser.add_argument("-comparable_attrs", "--comparable_attrs", type=str, default="")  # e.g. "ZIP_Code"
    parser.add_argument("-constant_ratio_low", "--constant_ratio_low", type=float, default=0.01)  # 0.02 for ncvoter
    parser.add_argument("-constant_ratio_high", "--constant_ratio_high", type=float, default=1.0)
    parser.add_argument("-seed", "--seed", type=int, default=42)
    args = parser.parse_args()
    arg_dict = args.__dict__

    data_path = arg_dict["data_dir"] + arg_dict["data_name"] + "_sample.csv"
    data = pd.read_csv(data_path)
    all_attrs = data.columns.tolist()

    revised_attrs = None
    if "airports" in arg_dict["data_name"]:
        revised_attrs = ["id", "elevation_ft"]
    elif "hospital" in arg_dict["data_name"]:
        revised_attrs = ["ZIP_Code", "Phone_Number"]
    elif "ncvoter" in arg_dict["data_name"]:
        revised_attrs = ["id", "county_id"]
    elif "tax" in arg_dict["data_name"]:
        revised_attrs = ["areacode", "phone", "zip", "salary", "singleexemp", "marriedexemp", "childexemp"]
    elif "aminer_merged_categorical" in arg_dict["data_name"]:
        revised_attrs = ["author2paper_id", "author_id", "paper_id", "author_id"]
    elif "aminer" in arg_dict["data_name"]:
        revised_attrs = ["author2paper_id", "author_id", "paper_id", "author_position"] + ["author_id", "published_papers", "citations", "h_index"] + ["paper_id", "year"]
    elif "AMiner_Author2Paper" in arg_dict["data_name"]:
        revised_attrs = ["author2paper_id", "author_id", "paper_id", "author_position"]
    elif "AMiner_Author" in arg_dict["data_name"]:
        revised_attrs = ["author_id", "published_papers", "citations", "h_index"]
    elif "AMiner_Paper" in arg_dict["data_name"]:
        revised_attrs = ["paper_id", "year"]
    elif "adults_categorical" in arg_dict["data_name"]:
        revised_attrs = ["age", "education_num", "capital_gain", "capital_loss", "hours_per_week"]
    elif "adults" in arg_dict["data_name"]:
        revised_attrs = ["age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week"]

    if revised_attrs is not None:
        for attr in revised_attrs:
            data[attr] = data[attr].astype(str).str.replace('\.0$', '').replace("nan", np.nan)

    # count and pick constants for constant predicate enumeration
    constants_filtered = pick_constants(data, all_attrs, arg_dict["constant_ratio_low"], arg_dict["constant_ratio_high"])

    all_predicates = enumerate_predicates(arg_dict["predicate_operators"].split(","),
                                          arg_dict["tuple_variable_num"],
                                          all_attrs,
                                          arg_dict["comparable_attrs"].split("||"),
                                          constants_filtered,
                                          arg_dict["data_name"])

    f = open(arg_dict["data_dir"] + "all_predicates.txt", "w")
    f.write("PAD\n")
    for pre in all_predicates:
        f.writelines(pre.print_predicate_new() + "\n")
    f.close()
