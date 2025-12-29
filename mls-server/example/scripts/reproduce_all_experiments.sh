#!/bin/bash

echo -e "------------ vary all for airports ------------"
./exp_airports_all.sh

echo -e "------------ vary all for hospital ------------"
./exp_hospital_all.sh

echo -e "------------ vary all for dblp ------------"
./exp_aminer_all.sh

echo -e "------------ vary all for inspection ------------"
./exp_inspection_all.sh

echo -e "------------ vary all for ncvoter ------------"
./exp_ncvoter_all.sh

echo -e "------------ vary all for adults ------------"
./exp_adults_all.sh
