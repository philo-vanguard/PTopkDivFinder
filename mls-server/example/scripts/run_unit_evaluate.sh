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



dataID=$1

inputRuledir=$2
echo -e ${inputRuledir}
inputRuleName=$3
echo -e ${inputRuleName}

tnum=$4
lambda=$5
processor=$6
w_fitness=$7
w_unexpectedness=$8
topK=$9
attributesUserInterested=${10}


predicateToIDFile="/tmp/rulefind/files_diversified_version2/"${task[${dataID}]}"/all_predicates.txt"
relevanceModelFile="/tmp/rulefind/files_diversified_version2/"${task[${dataID}]}"/model.txt"  # Mcorr
rulesUserAlreadyKnownFile="/tmp/rulefind/files_diversified_version2/"${task[${dataID}]}"/rulesUserAlreadyKnown.txt"
inputAllREEsPath="/tmp/rulefind/files_diversified_version2/all_rules_default_setting/"${task[${dataID}]}"_all_rules_supp0.000001_conf0.75.txt"


cd ..

resRootDir="./discoveryResultsTopKDiversifiedVersion2/evaluation/"

mkdir -p ${resRootDir}

resDir=${resRootDir}${task[${dataID}]}"/"

mkdir -p ${resDir}

echo -e "result output file dir: "${resDir}

outputFile_ptopkminer=${inputRuleName}_evaluation.txt

echo -e "result output file name: "${outputFile_ptopkminer}


if [ -f ${resDir}${outputFile_ptopkminer} ] 
then 
    echo "The file exists: "${resDir}${outputFile_ptopkminer}
else
    hdfs dfs -mkdir "/tmp/rulefind/rules_evaluate/"${task[${dataID}]}"/"
    inputREEsPath="/tmp/rulefind/rules_evaluate/"${task[${dataID}]}"/"${inputRuleName}".txt"
    hdfs dfs -rm ${inputREEsPath}
    hdfs dfs -put ${inputRuledir}${inputRuleName}".txt" ${inputREEsPath}

    ./run.sh support=0 confidence=0 dataset=${task[${dataID}]} taskID=${task[${dataID}]} numOfProcessors=${processor} algOption="evaluation_version2" maxTupleVariableNum=${tnum} inputREEsPath=${inputREEsPath} inputAllREEsPath=${inputAllREEsPath} outputResultFile=${outputFile_ptopkminer} lambda=${lambda} w_fitness=${w_fitness} w_unexpectedness=${w_unexpectedness} predicateToIDFile=${predicateToIDFile} relevanceModelFile=${relevanceModelFile} attributesUserInterested=${attributesUserInterested} rulesUserAlreadyKnownFile=${rulesUserAlreadyKnownFile} topK=${topK}

    rm ${resDir}${outputFile_ptopkminer}
    hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_ptopkminer} ${resDir}
fi
