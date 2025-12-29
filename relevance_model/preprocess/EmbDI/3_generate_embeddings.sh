#!/bin/bash

################### 3. ###################

for data_name in "airports" "hospital" "inspection" "ncvoter" "adults" "aminer"
do
  python -u main.py -f pipeline/config_files/config_${data_name}
done
