#!/bin/bash

source parameters.txt

export $(cut -d= -f1 parameters.txt)

envsubst  <config_tempalte.conf > neo4j.conf

echo "Created Neo4j Config"