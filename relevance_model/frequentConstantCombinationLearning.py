from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
import pandas as pd
import argparse
import os
import numpy as np


def main():
    parser = argparse.ArgumentParser(description="Learn the frequent constant predicate combinations")
    parser.add_argument('-data_name', '--data_name', type=str, default='airports')
    parser.add_argument('-data_path', '--data_path', type=str, default='../datasets/airports/airports_sample.csv')
    parser.add_argument('-output_dir', '--output_dir', type=str, default='../datasets/airports/')
    parser.add_argument('-supp_threshold', '--supp_threshold', type=float, default=0.5)
    args = parser.parse_args()
    arg_dict = args.__dict__

    if not os.path.exists(arg_dict['output_dir']):
        os.makedirs(arg_dict['output_dir'])

    data = pd.read_csv(arg_dict["data_path"])

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

    transactions = data.apply(lambda x: [f"{col}={val}" for col, val in x.items()], axis=1).tolist()

    te = TransactionEncoder()
    te_array = te.fit(transactions).transform(transactions)
    df_te = pd.DataFrame(te_array, columns=te.columns_)
    frequent_itemsets = apriori(df_te, min_support=arg_dict["supp_threshold"], use_colnames=True)
    print(frequent_itemsets)

    # filter nan
    frequent_constant_combinations = {}
    for index, row in frequent_itemsets.iterrows():
        contain = False
        for item in row["itemsets"]:
            if "nan" in item:
                contain = True
                break
            if arg_dict["data_name"] == "aminer" and ("paper_affiliations=" in item):
                contain = True
                break
        if contain is False:
            print(row["support"], list(row["itemsets"]))
            frequent_constant_combinations["^".join(list(row["itemsets"]))] = row["support"]

    print(frequent_constant_combinations)

    f = open(arg_dict["output_dir"] + "frequentConstantCombinations_" + str(arg_dict["supp_threshold"]) + ".txt", "w")
    for k, v in frequent_constant_combinations.items():
        f.write(k)
        f.write(", ")
        f.write("supp: ")
        f.write(str(v))
        f.write("\n")
    f.close()


if __name__ == '__main__':
    main()
