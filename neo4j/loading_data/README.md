# CSV to Neo4j

This repo contains the scripts that used to load CSV data to Neo4j.

## Input file

input config.yml has following structure

```

server_uri: bolt://127.0.0.1:7687/
admin_user: neo4j
admin_pass: neo4j
baseUrl: ./load-csv-python-test/neo4jloadcsv/
file_delimeter : ','
indexes:
  - CREATE CONSTRAINT ON (p:Account)
       ASSERT p.account_id IS UNIQUE;

  - CREATE CONSTRAINT ON (p:Order)
       ASSERT p.order_id IS UNIQUE;

steps:
  step1 :
      csv_dependent : True
      filename : account
      multi_process : True
      cql : |
          with $dict.rows as rows UNWIND rows as line
          MERGE (a:Account{account_id:line.account_id})
          SET a.start_date=line.start_date,
          a.status=line.status,
          a.ICD10=line.ICD10
step2 :
      csv_dependent : False
      filename : None
      cql : 
        - MATCH (ac:AccountCapacity)
          MATCH (a:Account{account_id:ac.account_id})
          MERGE (ac)<-[:HAS_CAPACITY]-(a);

        - MATCH (ord:Order)
          MATCH (ol:OrderLineDate {order_id:ord.order_id})
          MERGE (ol)-[:PART_OF]->(ord);
```
**server_uri**: The database server url use to load data into.<br />
**admin_user**: Username to access the database.<br />
**admin_pass**: Password to the server.<br />
**baseUrl**: Path to home directory of CSV's.<br />
**file_delimiter**: File delimiter to be used in CSV files.<br />
**indexes**: List of queries need to pre data load.<br />
**steps**: steps need to execute in an order.<br />
  **step1**: <br />
      **csv_dependent**: If step depend on csv file, it's True, otherwise False.<br />
      **filename**: Name of the dependent csv file, if csv_dependent is True, otherwise None.<br />
      **multi_process**: If step depend elgible for multiprocess then True, otherwise False.<br />
      **cql**: cql need to execute with load csv funcationality, It could be a list of cql if csv_dependent is false.<br />
    
## Deploy to prod
After modifying all necessary changes to configuration file, run following command 
```
python neo4j_csv.py config.yml
```