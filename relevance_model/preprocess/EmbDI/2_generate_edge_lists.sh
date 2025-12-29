#!/bin/bash

################### 2. ###################

# before running this scripts, 
# (1) replace " " with "##" in all datasets, by running datasets/remove_space.py
# (2) change the name EmbDI/logging.py to another name, after that, recover the name!


data_dir="/home/hanzy/embdi/datasets/"
for data_name in "airports_sample" "hospital_sample" "inspection_sample" "ncvoter_sample" "adults_sample" "aminer_sample"
do
  python -u EmbDI/edgelist.py -i ${data_dir}${data_name}/${data_name}_removeSpace.csv -o ${data_dir}${data_name}/edgelist.txt
done
