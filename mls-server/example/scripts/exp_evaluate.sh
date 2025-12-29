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



tupleNumDefault=2
lambda=2.0
numOfProcessorDefault=20


dataID=$1

dir=$2
files=$(ls $dir)

w_fitness=$3
w_unexpectedness=$4
topK=$5
attributesUserInterested=$6


for filename in $files
do
   if [[ $filename == *#* ]]
   then
      mv $dir$filename $dir${filename//#/_}
      echo "Input rule path: "$dir$filename
      filename=${filename//#/_}
      filename=${filename%.*}
      ./run_unit_evaluate.sh ${dataID} ${dir} ${filename} ${tupleNumDefault} ${lambda} ${numOfProcessorDefault} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}
   else
      echo "Input rule path: "$dir$filename
      filename=${filename%.*}
      ./run_unit_evaluate.sh ${dataID} ${dir} ${filename} ${tupleNumDefault} ${lambda} ${numOfProcessorDefault} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}
   fi
done

