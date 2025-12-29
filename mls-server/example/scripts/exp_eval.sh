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

topK=10
w_fitness=1
w_unexpectedness=1


dataID=3 # hospital
ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/random/hospital_vary_k/"
attributesUserInterested="City##Hospital_Type##Hospital_Owner##Emergency_Service##Condition"
./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

dataID=3 # hospital
ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/random/hospital_vary_supp/"
attributesUserInterested="City##Hospital_Type##Hospital_Owner##Emergency_Service##Condition"
./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

dataID=1 # airports
ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/random/airports_vary_conf/"
attributesUserInterested="type##name##iso_region##scheduled_service"
./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}



# dataID=3 # hospital
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/selector/"${task[${dataID}]}"/"
# attributesUserInterested="City##Hospital_Type##Hospital_Owner##Emergency_Service##Condition"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

# dataID=4 # inspection
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/selector/"${task[${dataID}]}"/"
# attributesUserInterested="Facility_Type##Risk##City##Inspection_Type##Results"
# # attributesUserInterested="Inspection_Date##Risk##City##Inspection_Type##Results"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}


# dataID=4 # inspection
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/random/inspection_vary_k/"
# attributesUserInterested="Facility_Type##Risk##City##Inspection_Type##Results"
# # attributesUserInterested="Inspection_Date##Risk##City##Inspection_Type##Results"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}


# -------------------------------- vary_supp & vary_conf --------------------------------
# dataID=3 # hospital
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/"${task[${dataID}]}"/"
# attributesUserInterested="City##Hospital_Type##Hospital_Owner##Emergency_Service##Condition"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

# dataID=1 # airports
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/"${task[${dataID}]}"/"
# attributesUserInterested="type##name##iso_region##scheduled_service"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}


# -------------------------------- vary supp --------------------------------
# dataID=4 # inspection
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/"${task[${dataID}]}"_vary_supp/"
# # attributesUserInterested="Facility_Type##Risk##City##Inspection_Type##Results"
# attributesUserInterested="Inspection_Date##Risk##Results"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

# # -------------------------------- vary k --------------------------------
# dataID=3 # hospital
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/"${task[${dataID}]}"/"
# attributesUserInterested="City##Hospital_Type##Hospital_Owner##Emergency_Service##Condition"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

# # -------------------------------- vary conf --------------------------------
# dataID=6 # aminer
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/"${task[${dataID}]}"/"
# attributesUserInterested="author_affiliations##published_papers##citations##h_index##p_index##p_index_with_unequal_a_index"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}


# -------------------------------- default setting --------------------------------
# dataID=0 # adults
# # ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/selector/"${task[${dataID}]}"/"
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/adults/vary_lambda/filterEnumNumber16_w_fitness1_w_unexp1/"
# attributesUserInterested="workclass##education##marital_status##relationship##hours_per_week##class"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}


# dataID=1 # airports
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/selector/"${task[${dataID}]}"/"
# attributesUserInterested="type##name##iso_region##scheduled_service"
# # attributesUserInterested="type##municipality"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

# dataID=3 # hospital
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/selector/"${task[${dataID}]}"/"
# attributesUserInterested="City##Hospital_Type##Hospital_Owner##Emergency_Service##Condition"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

# dataID=4 # inspection
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/selector/"${task[${dataID}]}"/"
# attributesUserInterested="Facility_Type##Risk##City##Inspection_Type##Results"
# # attributesUserInterested="Inspection_Date##Risk##City##Inspection_Type##Results"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

# dataID=5 # ncvoter
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/random/"${task[${dataID}]}"/"
# attributesUserInterested="election_phase##way_of_voting##voting_intention##party"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

# dataID=6 # aminer
# ruleDir="/opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/effectivess/selector/"${task[${dataID}]}"/"
# attributesUserInterested="author_affiliations##published_papers##citations##h_index##p_index##p_index_with_unequal_a_index"
# ./exp_evaluate.sh ${dataID} ${ruleDir} ${w_fitness} ${w_unexpectedness} ${topK} ${attributesUserInterested}

