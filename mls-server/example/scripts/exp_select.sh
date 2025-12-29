#!/bin/bash

echo -e "------------- TopkDivSelecter -------------"

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
filterEnumNumber=10

lambda=2.0
relevanceDefualt="fitness#unexpectedness"
diversityDefualt="rule_distance"
max_X_len_default=5
w_fitness=1
w_unexpectedness=1
rationality_low_threshold=0.1
rationality_high_threshold=0.95

expOption="select"



# -------------------------------- inspection --------------------------------
dataID=4
filterEnumNumber=10
# schema: "Inspection_ID,DBA_Name,AKA_Name,License,Facility_Type,Risk,Address,City,State,Zip,Inspection_Date,Inspection_Type,Results,Violations,Latitude,Longitude,Location"
attributesUserInterested="Facility_Type##Risk##City##Inspection_Type##Results"

expOption="select_vary_k"
if [ ${expOption} = "select_vary_k" ]
then
# for K in 1 10 20 30 40
for K in 1
do
    echo -e "supp = "${suppDefault}" and conf = "${confDefault}" with data "${task[${dataID}]}", relevance: "${relevanceDefualt}", diversity: "${diversityDefualt}
    ./run_unit_select.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${K} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested}
done
fi


# -------------------------------- hospital --------------------------------
dataID=3
filterEnumNumber=10
# schema: "Provider_Number,Hospital_Name,City,State,ZIP_Code,County_Name,Phone_Number,Hospital_Type,Hospital_Owner,Emergency_Service,Condition,Measure_Code,Measure_Name,Sample,StateAvg"
attributesUserInterested="City##Hospital_Type##Hospital_Owner##Emergency_Service##Condition"

expOption="select_vary_k"
if [ ${expOption} = "select_vary_k" ]
then
# for K in 1 10 20 30 40
for K in 1
do
    echo -e "supp = "${suppDefault}" and conf = "${confDefault}" with data "${task[${dataID}]}", relevance: "${relevanceDefualt}", diversity: "${diversityDefualt}
    ./run_unit_select.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${K} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested}
done
fi


# -------------------------------- airports --------------------------------
dataID=1
filterEnumNumber=10
# schema: "id,ident,type,name,latitude_deg,longitude_deg,elevation_ft,continent,iso_country,iso_region,municipality,scheduled_service,gps_code,iata_code,local_code,home_link,wikipedia_link,keywords"
attributesUserInterested="type##name##iso_region##scheduled_service"
./run_unit_select.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested}


# -------------------------------- adults --------------------------------
dataID=0
filterEnumNumber=16
# schema: "age,workclass,fnlwgt,education,education_num,marital_status,occupation,relationship,race,sex,capital_gain,capital_loss,hours_per_week,native_country,class"
attributesUserInterested="workclass##education##marital_status##relationship##hours_per_week##class"
./run_unit_select.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested}


# -------------------------------- hospital --------------------------------
dataID=3
filterEnumNumber=10
# schema: "Provider_Number,Hospital_Name,City,State,ZIP_Code,County_Name,Phone_Number,Hospital_Type,Hospital_Owner,Emergency_Service,Condition,Measure_Code,Measure_Name,Sample,StateAvg"
attributesUserInterested="City##Hospital_Type##Hospital_Owner##Emergency_Service##Condition"
./run_unit_select.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested}


# -------------------------------- inspection --------------------------------
dataID=4
filterEnumNumber=10
# schema: "Inspection_ID,DBA_Name,AKA_Name,License,Facility_Type,Risk,Address,City,State,Zip,Inspection_Date,Inspection_Type,Results,Violations,Latitude,Longitude,Location"
attributesUserInterested="Facility_Type##Risk##City##Inspection_Type##Results"
./run_unit_select.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested}



# -------------------------------- ncvoter --------------------------------
dataID=5
filterEnumNumber=100
# schema: "id,date,election_phase,way_of_voting,voting_intention,party,city_id,city,county_id,county_desc,city_id2,city_id3"
attributesUserInterested="election_phase##way_of_voting##voting_intention##party"
./run_unit_select.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested}



# -------------------------------- aminer --------------------------------
dataID=6
filterEnumNumber=10
# schema: "author_id,author_name,author_affiliations,published_papers,citations,h_index,p_index,p_index_with_unequal_a_index,research_interests,author2paper_id,paper_id,author_position,paper_title,author,paper_affiliations,year,venue"
attributesUserInterested="author_affiliations##published_papers##citations##h_index##p_index##p_index_with_unequal_a_index"
./run_unit_select.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested}

