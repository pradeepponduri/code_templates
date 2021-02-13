import multiprocessing as mp
from functools import wraps

import pandas as pd
from neo4j import GraphDatabase
import csv
import glob
import logging
import yaml
import datetime
import sys, os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


config_file=os.path.join(os.path.dirname(os.path.abspath(__file__)),sys.argv[1])

def db_connector(func):
    @wraps(func)
    def with_connection_(*args,**kwargs):
        try:
            config = load_config()
            _driver = GraphDatabase.driver(config['server_uri'], auth=(config['admin_user'], config['admin_pass']),encrypted=False)
            kwargs['_driver'] = _driver
            kwargs['config'] = config
            rv = func(*args,**kwargs)
            
        except Exception as e:
            print("Database connection error {0}".format(e))
            
        finally:
            _driver.close()
            return rv
        
    return with_connection_

@db_connector
def load_csv(fileName, cql,_driver=None,config=None,isParallel=False):
    """
    Read data from csv and load into database .
    Make sure delimiter flag in config is good.
    """
    #baseUrl = config['baseUrl']
    delimiter = config['file_delimiter']

    #fileKey =  + fileName + '.csv'
    fileKey = fileName
    print("File key {}", fileKey)
    print("chunk size {}",config['chunk_size'])
    cql = cql

    with _driver.session() as session:
        logging.info('Started Processing :  {0} with chunk size : {1}'.format(fileKey.split("/")[-1],config["chunk_size"]))
        row_chunks = pd.read_csv(fileKey, dtype=str, sep=delimiter, error_bad_lines=False,
                                    index_col=False,
                                    low_memory=False,
                                    chunksize=config['chunk_size'],
                                    skipinitialspace=True)
         #print("Processing file of Rows: ") 


        
        for data in row_chunks:
            rows_dict = {'rows': data.fillna(value="").to_dict('records')}
           # print(cql,rows_dict)
            session.run(statement=cql,dict=rows_dict).consume()
        
            
        logging.info('Completed Processing :  {0}'.format(fileKey.split("/")[-1]))
    print("{} : Completed file", datetime.datetime.utcnow())
    


@db_connector
def load_parquet(fileName, cql,_driver=None,config=None):
    """
    Read data from parquet and load into database.
    """
    with _driver.session() as session:
        df = pd.read_parquet(fileName, engine='fastparquet')
        df = df.to_dict(orient='records')
        session.run(statement=cql,dict=df).consume()

@db_connector
def run_cypher(queries,_driver=None,config=None):
    """
    Run raw cypher queries.
    """
    with _driver.session() as session:
        for query in queries:
            try:
                session.run(statement=query)
            except Exception as e:
                logging.error(e)
                pass

def task_submit(tasks, pool=None):
        
        if (pool):
            logging.info('Running with parallel')
            results_pool = [pool.apply_async(function_submit,task) for task in tasks]
            output = [p.get() for p in results_pool]
        else:
            result = [function_submit(task[0],*task[1:]) for task in tasks]

def function_submit(func,*args):
    logging.info('Started Task : '.format({func.__name__})   )
    result = func(*args)

def load_config(configuration=config_file):
    """
    Read config file from sys args and load configurations.
    """
    with open(configuration) as config_file:
        config = yaml.load(config_file)
    return config
def get_files_list(basefolder,folder_name,extention):
    print('Getting Files from ',folder_name,'/',folder_name,' with extenstion ',extention)
    return glob.glob("{0}/{1}/*.{2}".format(basefolder,folder_name,extention))

def main():

    config = load_config()
    exec_steps = config['steps']

    num_processors = mp.cpu_count()

    pre_load = []
    mp_tasks = []
    non_mp_tasks = []
    seq_tasks = []
    try:
        pre_load = [[run_cypher,config['indexes']]]
    except Exception as e:
        print(e)
        pass
    
    
    
    intm_mp_tasks = [[load_csv if(exec_steps[step]["file_type"]=='csv') else load_parquet, \
                      get_files_list(config["base_folder"],exec_steps[step]["filename"],exec_steps[step]["file_type"]),\
                      exec_steps[step]["cql"]] \
                for step in exec_steps if(\
                    (exec_steps[step]["csv_dependent"]==True) \
                and (exec_steps[step]["multi_process"]==True)\
                and (exec_steps[step]["process"]==True))]
    
    for i in range(len(intm_mp_tasks)):
      for j in (intm_mp_tasks[i][1]):
          mp_tasks.append([intm_mp_tasks[i][0],j,intm_mp_tasks[i][2]])
          
    
    
    
    intm_non_mp_tasks = [[load_csv,get_files_list(config["base_folder"],exec_steps[step]["filename"],exec_steps[step]["file_type"]),exec_steps[step]["cql"]] \
                    for step in exec_steps if(\
                        (exec_steps[step]["csv_dependent"]==True) \
                    and (exec_steps[step]["multi_process"]==False) \
                    and (exec_steps[step]["process"]==True) )]
    for i in range(len(intm_non_mp_tasks)):
          for j in (intm_non_mp_tasks[i][1]):
              non_mp_tasks.append([intm_non_mp_tasks[i][0],j,intm_non_mp_tasks[i][2]])
    
    seq_tasks = [[run_cypher,exec_steps[step]["cql"]] \
                    for step in exec_steps if((exec_steps[step]["csv_dependent"]==False) and (exec_steps[step]["process"]==True) )]

    try:
       logging.info('CREATING INDEXES')
       task_submit(pre_load)
       logging.info('INDEXES CREATED')
    except:
       pass
        
    #print(non_mp_tasks)
    with mp.Pool(num_processors) as pool:
          task_submit(mp_tasks,pool)

    task_submit(non_mp_tasks)
    #task_submit(mp_tasks)
    task_submit(seq_tasks)
    
if __name__ == "__main__":
    main()
