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

relevanceMeasures=(
"fitness"
"unexpectedness"
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
predictConfThreshold=0.9
max_X_len_default=5
w_fitness=1
w_unexpectedness=1
rationality_low_threshold=0.1 # ignore
rationality_high_threshold=0.95 # ignore


dataID=1  # airports
# schema: "id,ident,type,name,latitude_deg,longitude_deg,elevation_ft,continent,iso_country,iso_region,municipality,scheduled_service,gps_code,iata_code,local_code,home_link,wikipedia_link,keywords"
attributesUserInterested="type##name##iso_region##scheduled_service"



expOption="default"
./run_unit.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${filterEnumNumber} ${relevanceDefualt} ${diversityDefualt} ${lambda} ${w_fitness} ${w_unexpectedness} ${rationality_low_threshold} ${rationality_high_threshold} ${max_X_len_default} ${attributesUserInterested} ${predictConfThreshold}
