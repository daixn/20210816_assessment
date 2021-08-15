import pandas as pd
import os
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


def telephone_numbers(config):
    logging.basicConfig(filename="{}{}_telephone_numbers.log".format(config['telephone_numbers']['logging_path'],
                                                        datetime.datetime.now().strftime('%Y%m%dT%H%M%S')), 
                        level=logging.DEBUG,
                        force=True)
    logger=logging.getLogger()
    
    try:
        df = __read_files(config['telephone_numbers']['raw_files'])
        logger.info("Customer Details files read successfully") 
        
        df = __explode_customer_details(df)
        telephone_df = df[['user_id', 'telephone_numbers']]
        telephone_df = telephone_df.explode('telephone_numbers')
        logger.info("COLUMNS normalized and exploded successfully")
        
        telephone_df['processed_dt'] = datetime.datetime.now()
        logger.info("PROCESSED DT generated successfully")

        output_method(telephone_df, config, 'telephone_numbers')
        logger.info("OUTPUT generated successfully")
        logger.info("###PROCESS COMPLETED###")
        logging.shutdown()        
    except Exception as E:
        logger.critical("Process failed: Error message {}".format(E)) 
        logging.shutdown()