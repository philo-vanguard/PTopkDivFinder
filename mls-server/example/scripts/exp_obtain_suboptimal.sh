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
"adults_categorical"
"adults_small"
"airports_small"
"inspection_small"
"hospital_small"
"ncvoter_small"
)


tupleNumDefault=2
lambda=2.0
numOfProcessorDefault=20


dataID=$1

dir=$2
files=$(ls $dir)

w_fitness=$3
w_unexpectedness=$4
topKNum=$5
iter_subopt=$6
attributesUserInterested=$7

for filename in $files
do
   if [[ $filename == *#* ]]
   then
      mv $dir$filename $dir${filename//#/_}
      echo "Input rule path: "$dir$filename

      if [[ $filename == *noLB* ]]
      then
         echo -e "we do not evaluate the results of noLB_noUB baseline"
      else
         filename=${filename//#/_}
         filename=${filename%.*}
         ./run_unit_obtain_suboptimal.sh ${dataID} ${dir} ${filename} ${tupleNumDefault} ${lambda} ${numOfProcessorDefault} ${w_fitness} ${w_unexpectedness} ${topKNum} ${iter_subopt} ${attributesUserInterested}
      fi
   else
      if [[ $filename == *baseline* ]] || [[ $filename == *all* ]]
      then
         echo "Input rule path: "$dir$filename
         filename=${filename%.*}
         ./run_unit_obtain_suboptimal.sh ${dataID} ${dir} ${filename} ${tupleNumDefault} ${lambda} ${numOfProcessorDefault} ${w_fitness} ${w_unexpectedness} ${topKNum} ${iter_subopt} ${attributesUserInterested}
      else
         echo -e "evaluated already: "$filename
      fi
   fi
done

