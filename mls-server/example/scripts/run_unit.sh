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
predictConfThreshold=${18}




predicateToIDFile="/tmp/rulefind/files_diversified/"${task[${dataID}]}"/all_predicates.txt"
relevanceModelFile="/tmp/rulefind/files_diversified/"${task[${dataID}]}"/model.txt"  # Mcorr
constantCombinationFile="/tmp/rulefind/files_diversified/"${task[${dataID}]}"/frequentConstantCombinations.txt"
rulesUserAlreadyKnownFile="/tmp/rulefind/files_diversified/"${task[${dataID}]}"/rulesUserAlreadyKnown.txt"
validityModelFile="/tmp/rulefind/files_diversified/"${task[${dataID}]}"/xgboost_model.bin"
if [[ "$expOption" == "vary_supp" ]] || [[ "$expOption" == "vary_conf" ]]
then
    validityModelFile="/tmp/rulefind/files_diversified/"${task[${dataID}]}"/xgboost_model_supp"${supp}"_conf"${conf}".bin"
fi

if [ $dataID -eq 13 ] || [ $dataID -eq 14 ] || [ $dataID -eq 15 ] || [ $dataID -eq 16 ] || [ $dataID -eq 17 ]  # ncvoter, vary data size
then
    predicateToIDFile="/tmp/rulefind/files_diversified/ncvoter/all_predicates.txt"
    relevanceModelFile="/tmp/rulefind/files_diversified/ncvoter/model.txt"
    constantCombinationFile="/tmp/rulefind/files_diversified/ncvoter/frequentConstantCombinations.txt"
    validityModelFile="/tmp/rulefind/files_diversified/ncvoter/xgboost_model.bin"
    rulesUserAlreadyKnownFile="/tmp/rulefind/files_diversified/ncvoter/rulesUserAlreadyKnown.txt"
fi


cd ..

resRootDir="./discoveryResultsTopKDiversified/"

mkdir -p ${resRootDir}

resDir=${resRootDir}${task[${dataID}]}"/filterEnumNumber"${filterEnumNumber}"_w_fitness"${w_fitness}"_w_unexp"${w_unexpectedness}"_lambda"${lambda}"/"

if [[ "$expOption" == "vary_lambda" ]]
then
    resDir=${resRootDir}${task[${dataID}]}"/vary_lambda/filterEnumNumber"${filterEnumNumber}"_w_fitness"${w_fitness}"_w_unexp"${w_unexpectedness}"/"
fi

mkdir -p ${resDir}

echo -e "result output file: "${resDir}

tailFile="_len"${MAX_X_LENGTH}"_supp"${supp}"_conf"${conf}"_top"${topK}"_processor"${processor}"_rel_"${relevanceMeasures}"__div_"${diversityMeasures}".txt"
tailFileCheck="_len"${MAX_X_LENGTH}"_supp"${supp}"_conf"${conf}"_top"${topK}"_processor"${processor}"_rel_"${relevanceMeasures//#/_}"__div_"${diversityMeasures//#/_}".txt"

if [[ "$expOption" == "vary_lambda" ]]
then
    tailFile="_len"${MAX_X_LENGTH}"_supp"${supp}"_conf"${conf}"_top"${topK}"_processor"${processor}"_rel_"${relevanceMeasures}"__div_"${diversityMeasures}"_lambda"${lambda}".txt"
fi

outputFile_ptopkminer='result_'${task[${dataID}]}"_"${expOption}${tailFile}
outputFile_ptopkminer_nop='result_'${task[${dataID}]}"_nop_"${expOption}${tailFile}
outputFile_ptopkminer_noOpt='result_'${task[${dataID}]}"_noOpt_"${expOption}${tailFile}
outputFile_ptopkminer_nop_noOpt='result_'${task[${dataID}]}"_nop_noOpt_"${expOption}${tailFile}



echo -e "-------------------- PTopkDivFinder algorithm --------------------"
echo -e "output file name : "${outputFile_ptopkminer}

if [ -f ${resDir}${outputFile_ptopkminer} ] 
then 
    echo "The file exists: "${resDir}${outputFile_ptopkminer}
else
    ./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} round=1 maxTupleVariableNum=${tnum} ifPrune=true outputResultFile=${outputFile_ptopkminer} algOption="diversified" version=2 numOfProcessors=${processor} MLOption=1 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} MAX_X_LENGTH=${MAX_X_LENGTH} lambda=${lambda} relevance=${relevanceMeasures} diversity=${diversityMeasures} w_fitness=${w_fitness} w_unexpectedness=${w_unexpectedness} rationality_low_threshold=${rationality_low_threshold} rationality_high_threshold=${rationality_high_threshold} predicateToIDFile=${predicateToIDFile} relevanceModelFile=${relevanceModelFile} attributesUserInterested=${attributesUserInterested} rulesUserAlreadyKnownFile=${rulesUserAlreadyKnownFile} constantCombinationFile=${constantCombinationFile} validityModelFile=${validityModelFile} predictConfThreshold=${predictConfThreshold} ifConstantOffline=true ifCheckValidityByMvalid=true

    rm ${resDir}${outputFile_ptopkminer}
    hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_ptopkminer} ${resDir}
fi



echo -e "-------------------- PTopkDivFinder-nop algorithm --------------------"
echo -e "output file name : "${outputFile_ptopkminer_nop}

if [ -f ${resDir}${outputFile_ptopkminer_nop} ] 
then 
    echo "The file exists: "${resDir}${outputFile_ptopkminer_nop}
else
    ./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} round=1 maxTupleVariableNum=${tnum} ifPrune=true outputResultFile=${outputFile_ptopkminer_nop} algOption="diversified" version=2 numOfProcessors=${processor} MLOption=1 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} MAX_X_LENGTH=${MAX_X_LENGTH} lambda=${lambda} relevance=${relevanceMeasures} diversity=${diversityMeasures} w_fitness=${w_fitness} w_unexpectedness=${w_unexpectedness} rationality_low_threshold=${rationality_low_threshold} rationality_high_threshold=${rationality_high_threshold} predicateToIDFile=${predicateToIDFile} relevanceModelFile=${relevanceModelFile} attributesUserInterested=${attributesUserInterested} rulesUserAlreadyKnownFile=${rulesUserAlreadyKnownFile} constantCombinationFile=${constantCombinationFile} validityModelFile=${validityModelFile} predictConfThreshold=${predictConfThreshold} ifConstantOffline=true ifCheckValidityByMvalid=false

    rm ${resDir}${outputFile_ptopkminer_nop}
    hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_ptopkminer_nop} ${resDir}
fi



echo -e "-------------------- PTopkDivFinder-noOpt algorithm --------------------"
echo -e "output file name : "${outputFile_ptopkminer_noOpt}

if [ -f ${resDir}${outputFile_ptopkminer_noOpt} ] 
then 
    echo "The file exists: "${resDir}${outputFile_ptopkminer_noOpt}
else
    ./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} round=1 maxTupleVariableNum=${tnum} ifPrune=true outputResultFile=${outputFile_ptopkminer_noOpt} algOption="diversified" version=2 numOfProcessors=${processor} MLOption=1 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} MAX_X_LENGTH=${MAX_X_LENGTH} lambda=${lambda} relevance=${relevanceMeasures} diversity=${diversityMeasures} w_fitness=${w_fitness} w_unexpectedness=${w_unexpectedness} rationality_low_threshold=${rationality_low_threshold} rationality_high_threshold=${rationality_high_threshold} predicateToIDFile=${predicateToIDFile} relevanceModelFile=${relevanceModelFile} attributesUserInterested=${attributesUserInterested} rulesUserAlreadyKnownFile=${rulesUserAlreadyKnownFile} constantCombinationFile=${constantCombinationFile} validityModelFile=${validityModelFile} predictConfThreshold=${predictConfThreshold} ifConstantOffline=false ifCheckValidityByMvalid=true

    rm ${resDir}${outputFile_ptopkminer_noOpt}
    hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_ptopkminer_noOpt} ${resDir}
fi



echo -e "-------------------- PTopkDivFinder-nop-noOpt algorithm --------------------"
echo -e "output file name : "${outputFile_ptopkminer_nop_noOpt}

if [ -f ${resDir}${outputFile_ptopkminer_nop_noOpt} ] 
then 
    echo "The file exists: "${resDir}${outputFile_ptopkminer_nop_noOpt}
else
    ./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} round=1 maxTupleVariableNum=${tnum} ifPrune=true outputResultFile=${outputFile_ptopkminer_nop_noOpt} algOption="diversified" version=2 numOfProcessors=${processor} MLOption=1 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} MAX_X_LENGTH=${MAX_X_LENGTH} lambda=${lambda} relevance=${relevanceMeasures} diversity=${diversityMeasures} w_fitness=${w_fitness} w_unexpectedness=${w_unexpectedness} rationality_low_threshold=${rationality_low_threshold} rationality_high_threshold=${rationality_high_threshold} predicateToIDFile=${predicateToIDFile} relevanceModelFile=${relevanceModelFile} attributesUserInterested=${attributesUserInterested} rulesUserAlreadyKnownFile=${rulesUserAlreadyKnownFile} constantCombinationFile=${constantCombinationFile} validityModelFile=${validityModelFile} predictConfThreshold=${predictConfThreshold} ifConstantOffline=false ifCheckValidityByMvalid=false

    rm ${resDir}${outputFile_ptopkminer_nop_noOpt}
    hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_ptopkminer_nop_noOpt} ${resDir}
fi

