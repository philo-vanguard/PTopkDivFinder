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
"ncvoter_0.2"
"ncvoter_0.4"
"ncvoter_0.6"
"ncvoter_0.8"
"ncvoter_1.0"
)

exp=(
"vary_n"
"vary_supp"
"vary_conf"
"vary_k"
"vary_tuples"
"vary_topk"
"vary_synD"
"vary_synD_n"
)


dataID=$1

expOption=$2

supp=$3
conf=$4
topK=$5
tnum=$6
processor=$7

filterEnumNumber=$8

relevanceMeasures=$9
diversityMeasures=${10}
lambda=${11}

w_fitness=${12}
w_unexpectedness=${13}
rationality_low_threshold=${14}
rationality_high_threshold=${15}
MAX_X_LENGTH=${16}

attributesUserInterested=${17}


predicateToIDFile="/tmp/rulefind/files_diversified_version2/"${task[${dataID}]}"/all_predicates.txt"
relevanceModelFile="/tmp/rulefind/files_diversified_version2/"${task[${dataID}]}"/model.txt"  # Mcorr
rulesUserAlreadyKnownFile="/tmp/rulefind/files_diversified_version2/"${task[${dataID}]}"/rulesUserAlreadyKnown.txt"
inputAllREEsPath="/tmp/rulefind/files_diversified_version2/all_rules_default_setting/"${task[${dataID}]}"_all_rules_supp0.000001_conf0.75.txt"

if [ $dataID -eq 13 ] || [ $dataID -eq 14 ] || [ $dataID -eq 15 ] || [ $dataID -eq 16 ] || [ $dataID -eq 17 ]  # ncvoter, vary data size
then
    predicateToIDFile="/tmp/rulefind/files_diversified_version2/ncvoter/all_predicates.txt"
    relevanceModelFile="/tmp/rulefind/files_diversified_version2/ncvoter/model.txt"
    rulesUserAlreadyKnownFile="/tmp/rulefind/files_diversified_version2/ncvoter/rulesUserAlreadyKnown.txt"
    inputAllREEsPath="/tmp/rulefind/files_diversified_version2/all_rules_default_setting/ncvoter_all_rules_supp0.000001_conf0.75.txt"
fi


cd ..

resRootDir="./discoveryResultsTopKDiversifiedVersion2/"

mkdir -p ${resRootDir}

resDir=${resRootDir}${task[${dataID}]}"/filterEnumNumber"${filterEnumNumber}"_w_fitness"${w_fitness}"_w_unexp"${w_unexpectedness}"_lambda"${lambda}"/"

mkdir -p ${resDir}

echo -e "result output file: "${resDir}

tailFile="_len"${MAX_X_LENGTH}"_supp"${supp}"_conf"${conf}"_top"${topK}"_processor"${processor}"_rel_"${relevanceMeasures}"__div_"${diversityMeasures}".txt"
tailFileCheck="_len"${MAX_X_LENGTH}"_supp"${supp}"_conf"${conf}"_top"${topK}"_processor"${processor}"_rel_"${relevanceMeasures//#/_}"__div_"${diversityMeasures//#/_}".txt"

outputFile_ptopkminer='result_'${task[${dataID}]}"_"${expOption}${tailFile}


echo -e "-------------------- PTopkDivSelecter algorithm --------------------"
echo -e "output file name : "${outputFile_ptopkminer}

if [ -f ${resDir}${outputFile_ptopkminer} ] 
then 
    echo "The file exists: "${resDir}${outputFile_ptopkminer}
else
    ./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} round=1 maxTupleVariableNum=${tnum} ifPrune=true outputResultFile=${outputFile_ptopkminer} algOption="diversified_from_rule_set" version=2 numOfProcessors=${processor} MLOption=1 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} MAX_X_LENGTH=${MAX_X_LENGTH} lambda=${lambda} relevance=${relevanceMeasures} diversity=${diversityMeasures} w_fitness=${w_fitness} w_unexpectedness=${w_unexpectedness} rationality_low_threshold=${rationality_low_threshold} rationality_high_threshold=${rationality_high_threshold} predicateToIDFile=${predicateToIDFile} relevanceModelFile=${relevanceModelFile} attributesUserInterested=${attributesUserInterested} rulesUserAlreadyKnownFile=${rulesUserAlreadyKnownFile} inputAllREEsPath=${inputAllREEsPath}

    rm ${resDir}${outputFile_ptopkminer}
    hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_ptopkminer} ${resDir}
fi


