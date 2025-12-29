#!/bin/bash

echo -e "varying all for adults"

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
"ncvoter_0.2"
"ncvoter_0.4"
"ncvoter_0.6"
"ncvoter_0.8"
"ncvoter_1.0"
)

relevanceMeasures=(
"fitness"
"unexpectedness"
"rationality"
)

diversityMeasures=(
"rule_distance"
)


suppDefault=0.000001
confDefault=0.75
topKDefault=10
tupleNumDefault=2
numOfProcessorDefault=20
filterEnumNumber=16

lambda=2.0
relevanceDefualt="fitness#unexpectedness"
diversityDefualt="rule_distance"
predictConfThreshold=0.9
max_X_len_default=5
w_fitness=1
w_unexpectedness=1
rationality_low_threshold=0.1 # ignore
rationality_high_threshold=0.95 # ignore

# -------------------------------- adults --------------------------------
dataID=0
# schema: "age,workclass,fnlwgt,education,education_num,marital_status,occupation,relationship,race,sex,capital_gain,capital_loss,hours_per_week,native_country,class"
attributesUserInterested="workclass##education##marital_status##relationship##hours_per_week##class"


expOption="default"
./run_unit.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested} ${predictConfThreshold}


expOption="vary_supp"
if [ ${expOption} = "vary_supp" ]
then
for supp in 0.1 0.01 0.0001 0.000001 0.00000001
do
    echo -e "supp = "${supp}" and conf = "${confDefault}" with data "${task[${dataID}]}", relevance: "${relevanceDefualt}", diversity: "${diversityDefualt}
    ./run_unit.sh ${dataID} ${expOption} ${supp} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested} ${predictConfThreshold}
done
fi



expOption="vary_conf"
if [ ${expOption} = "vary_conf" ]
then
for conf in 0.95 0.9 0.85 0.8 0.75
do
    echo -e "supp = "${suppDefault}" and conf = "${conf}" with data "${task[${dataID}]}", relevance: "${relevanceDefualt}", diversity: "${diversityDefualt}
    ./run_unit.sh ${dataID} ${expOption} ${suppDefault} ${conf} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested} ${predictConfThreshold}
done
fi


expOption="vary_k"
if [ ${expOption} = "vary_k" ]
then
for K in 1 10 20 30 40
do
    echo -e "supp = "${suppDefault}" and conf = "${confDefault}" with data "${task[${dataID}]}", relevance: "${relevanceDefualt}", diversity: "${diversityDefualt}
    ./run_unit.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${K} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested} ${predictConfThreshold}
done
fi


expOption="vary_len"
if [ ${expOption} = "vary_len" ]
then
for max_X_len in 1 2 3 4 5 6 7 8 9 10
do
    echo -e "supp = "${suppDefault}" and conf = "${confDefault}" with data "${task[${dataID}]}", relevance: "${relevanceDefualt}", diversity: "${diversityDefualt}
    ./run_unit.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len} ${attributesUserInterested} ${predictConfThreshold}
done
fi

expOption="vary_lambda"
if [ ${expOption} = "vary_lambda" ]
then
for lambda_ in 0.222 0.5 0.857 1.333 2 3 4.667 8 18 # corresponding lambda in paper: 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9
do
    echo -e "supp = "${suppDefault}" and conf = "${confDefault}" with data "${task[${dataID}]}", relevance: "${relevanceDefualt}", diversity: "${diversityDefualt}
    ./run_unit.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda_} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested} ${predictConfThreshold}
done
fi
