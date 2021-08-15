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



@invalid_info_in_column(columns =  ['created_at', 'updated_at'])
def __clean_time(df, columns, if_false = None):    
    def cleaning_logic(an_input):
        try:
            an_input = an_input.replace('T', ' ')
            an_input = an_input.rsplit(':',1)[0] +':' + an_input.rsplit(':',1)[1][:2]
            return datetime.datetime. strptime(an_input, '%Y-%m-%d %H:%M:%S' )
        except:
            return if_false
    
    for column in columns:
        df[column] = df[column].map(lambda x: cleaning_logic(x))
    
    return df


def users(config):    
    logging.basicConfig(filename="{}{}_users.log".format(config['users']['logging_path'],
                                                        datetime.datetime.now().strftime('%Y%m%dT%H%M%S')), 
                        level=logging.DEBUG,
                        force=True)
    logger=logging.getLogger() 
    try:
        df = __read_files(config['jobs_history']['raw_files'])
        logger.info("Customer Details files read successfully") 
        
        backup_df = df.copy(deep = True)
        logger.info("Customer Details deep copied successfully") 
        
        df = __explode_customer_details(df)    
        users_df = df[['user_id', 'created_at', 'updated_at', 'logged_at', 'is_active',
           'car_license_plate', 'car_brand', 'name', 'dob', 'address', 'username',
           'password', 'national_id']]
        logger.info("COLUMNS normalized and exploded successfully")
        

        users_df['logged_at'] = users_df['logged_at'].map(lambda x: datetime.datetime.fromtimestamp(x))
        users_df = __clean_time(users_df, ['created_at', 'updated_at'])
        logger.info("TIMESTAMP cleaned successfully")
        
        users_df['processed_dt'] = generate_processed_dt()
        logger.info("PROCESSED DT generated successfully")        
        
        clean_df, rejected_df = clean_error_data(backup_df, users_df)
        
        discard_data(rejected_df, config['users']['invalid_data'], 'users')
        logger.info("REJECTED data moved to INVALID folder successfully")    


        clean_df['address'] = clean_df['address'].map(lambda x: clean_crlf(x))
        logger.info("Line break and Carriage Return cleaned successfully")
        
        clean_df = fernet_masking(clean_df, 
                                  ['address', 'username', 'password', 'national_id'],
                                  config['users']['masking_key'])
        logger.info("Sensitive columns masked successfully")

        clean_df['dob'] = pd.to_datetime(clean_df['dob'], format='%Y-%m-%d')
        
        output_method(clean_df, config, 'users')
        logger.info("OUTPUT generated successfully")
        logger.info("###PROCESS COMPLETED###")
        logging.shutdown()
    except Exception as E:
        logger.critical("Process failed: Error message {}".format(E)) 
        logging.shutdown()