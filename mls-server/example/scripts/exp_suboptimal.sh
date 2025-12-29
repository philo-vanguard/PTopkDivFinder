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

topKNum=10

iter_subopt=100


dataID=3 # hospital
attributesUserInterested="City##Hospital_Type##Hospital_Owner##Emergency_Service##Condition"
./exp_obtain_suboptimal.sh ${dataID} /opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/allRules_constantFilterRatio5/default/${task[${dataID}]}/ 1 1 ${topKNum} ${iter_subopt} ${attributesUserInterested}


dataID=1 # airports
attributesUserInterested="type##name##iso_region##scheduled_service"
./exp_obtain_suboptimal.sh ${dataID} /opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/allRules_constantFilterRatio5/default/${task[${dataID}]}/ 1 1 ${topKNum} ${iter_subopt} ${attributesUserInterested}


dataID=4 # inspection
attributesUserInterested="Facility_Type##Risk##City##Inspection_Type##Results"
./exp_obtain_suboptimal.sh ${dataID} /opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/allRules_constantFilterRatio5/default/${task[${dataID}]}/ 1 1 ${topKNum} ${iter_subopt} ${attributesUserInterested}


dataID=0 # adults
attributesUserInterested="workclass##education##marital_status##relationship##hours_per_week##class"
./exp_obtain_suboptimal.sh ${dataID} /opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/allRules_constantFilterRatio5/default/${task[${dataID}]}/ 1 1 ${topKNum} ${iter_subopt} ${attributesUserInterested}


dataID=6 # aminer
attributesUserInterested="author_affiliations##published_papers##citations##h_index##p_index##p_index_with_unequal_a_index"
./exp_obtain_suboptimal.sh ${dataID} /opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/allRules_constantFilterRatio5/default/${task[${dataID}]}/ 1 1 ${topKNum} ${iter_subopt} ${attributesUserInterested}


dataID=5 # ncvoter
attributesUserInterested="election_phase##way_of_voting##voting_intention##party"
./exp_obtain_suboptimal.sh ${dataID} /opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedVersion2/allRules_constantFilterRatio5/default/${task[${dataID}]}/ 1 1 ${topKNum} ${iter_subopt} ${attributesUserInterested}



# for dataID in 22 20 21 19 23
# do
#     ./exp_obtain_suboptimal.sh ${dataID} /opt/disk1/hanzy/discovery/discoveryResultsTopKDiversifiedShepherd/obtainSuboptimal/${task[${dataID}]}/ 1 1 ${topKNum} ${iter_subopt} ${attributesUserInterested}
# done

