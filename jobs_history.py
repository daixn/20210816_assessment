import pandas as pd
import os
import datetime
from common import *
import logging

def __read_files(path):
    list_of_customer_details_files = []
    for file in os.listdir(path):
        if file.endswith('.json'):
            list_of_customer_details_files.append(pd.read_json(path + file, lines=True))
    customer_details_df = pd.concat(list_of_customer_details_files)    
    return customer_details_df
    

def __explode_customer_details(df):
    df = pd.concat([df.drop('user_details', axis=1), pd.DataFrame(df['user_details'].tolist())], axis=1)
    df['jobs_history'] = df['jobs_history'].map(lambda x: x[0])
    df  = pd.concat([df.drop('jobs_history', axis=1),pd.json_normalize(df['jobs_history'])], axis=1)
    return df



def jobs_history(config):
    logging.basicConfig(filename="{}{}_jobs_history.log".format(config['jobs_history']['logging_path'],
                                                        datetime.datetime.now().strftime('%Y%m%dT%H%M%S')), 
                        level=logging.DEBUG,
                        force=True)
    logger=logging.getLogger()
    try:
        df = __read_files(config['jobs_history']['raw_files'])
        logger.info("Customer Details files read successfully") 
        
        df = __explode_customer_details(df)
        jobs_df = df[['id', 'user_id', 'occupation',
                           'is_fulltime','start', 'end']]
        logger.info("COLUMNS normalized and exploded successfully")
        
        jobs_df['processed_dt'] = datetime.datetime.now()
        logger.info("PROCESSED DT generated successfully")

        output_method(jobs_df, config, 'jobs_history')
        logger.info("OUTPUT generated successfully")
        logger.info("###PROCESS COMPLETED###")
        logging.shutdown()
        
    except Exception as E:
        logger.critical("Process failed: Error message {}".format(E)) 
        logging.shutdown()