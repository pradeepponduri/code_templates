#!/bin/bash

source parameters.txt

#export data_path plugins_path logs_path trasanctions_path import_path
export $(cut -d= -f1 parameters.txt)
#sed -e 's/:[^:\/\/]/="/g;s/$/"/g;s/ *=/=/g' parameters.yaml 

echo $data_path
envsubst  <config_tempalte.conf > neo4j.conf