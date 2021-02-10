# Create Neo4j Configuration File


This code create a configuration file for neo4j with provided parameters in parameters.txt. 

## Files and description

> config_tempalte.conf : A Template configuration file for neo4j.
 
> paramertes.conf : parameters that can be changed to replce in template neo4j.conf. 

> neo4j.conf : configuration file that can be used to start neo4j database. (APOC and ALOG pluging enabled) 

> create_config.sh : Shell file to generate end file (neo4j.conf)

## Usage

```sh
$ sh create_config.sh
# creates/replace file : neo4j.conf
```