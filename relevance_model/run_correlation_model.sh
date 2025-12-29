#!/bin/bash
task=(
"adults"
"airports"
"flight"
"hospital"
"inspection"
"ncvoter"
"aminer"
"tax100w"
"tax200w"
"tax400w"
"tax600w"
"tax800w"
"tax1000w"
)

for data_name in "airports" "hospital" "inspection" "ncvoter" "adults" "aminer"
do
    rules_file="../datasets/"${data_name}"/rules.txt"
    all_predicates_file="../datasets/"${data_name}"/all_predicates.txt"
    for ratio in "0.1_0.5" "0.3_0.7" "0.3_0.8" "0.4_0.8" "0.5_0.8"  # the low/high threshold for snorkel labelling
    do
        for time in 1 2 3 4 5 # running times
        do
            train_test_dir="../datasets/"${data_name}"/train_"${ratio}"/"
            train_file=${train_test_dir}"/train.csv"
            test_file=${train_test_dir}"/test.csv"
            pretrained_matrix_file=${train_test_dir}"/predicateEmbedds.csv"
            output_model_dir=${train_test_dir}"/model_${time}/"
            mkdir -p ${output_model_dir}
            # 1. pre-train and generate pretrained_matrix_file
            python -u relevanceFixedEmbeds_main.py -rules_file ${rules_file} -test_file ${test_file} -train_file ${train_file} -output_model_dir ${output_model_dir} -pretrained_matrix_file ${pretrained_matrix_file} -all_predicates_file ${all_predicates_file}
            # 2. fine-tune
            python -u relevanceFixedEmbeds_main.py -rules_file ${rules_file} -test_file ${test_file} -train_file ${train_file} -output_model_dir ${output_model_dir} -pretrained_matrix_file ${pretrained_matrix_file} -all_predicates_file ${all_predicates_file}
        done
    done
done
