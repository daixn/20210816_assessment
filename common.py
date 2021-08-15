import pandas as pd
import os
import datetime
from functools import reduce
from sqlalchemy import create_engine
from cryptography.fernet import Fernet

def invalid_info_in_column(columns):
    def decorator_executor(func):
        def inner(*args, **kwargs):
            '''
             inner function is to identify column with invalid input
            '''
            df = func(*args, **kwargs)
            for column in columns:
                df[column] = df[column].fillna('ERROR: invalid info')
            return df
        return inner
    return decorator_executor

def timestamp_checker(an_input, if_false = None):
    """
    this checking checks if the input fits a timestamp format
    which is positive number of integer/string
    
    if criteria failed to meet will return if_false value
    """
    
    if (type(an_input) in [float, int]):
        try:
            if an_input > 0:
                return int(an_input)
        except:
            return if_false
    elif type(an_input) == str:
        if an_input.isnumeric():
            if int(an_input) > 0:
                return int(an_input)
            else:
                return if_false
        else:
            return if_false
    else:
        return if_false
    
    
    
def clean_crlf(an_input):
    """
    remove line break and carriage return in longstring
    """
    if type(an_input) == str:
        return an_input.replace('\r', ' ').replace('\n', ' ')
    else:
        pass



def clean_error_data(original_df , 
                     processed_df, 
                     output_column_name = 'DATAERROR', 
                     key_to_check = 'ERROR: invalid info'):
    
    processed_df[output_column_name] = ''
    for column in processed_df.columns:
        processed_df[output_column_name] = processed_df.apply(lambda x: x[output_column_name] + ' ' + column if x[column] == key_to_check else x[output_column_name], 1)
    processed_df[output_column_name] = processed_df[output_column_name].map(lambda x: "erroneous data in " +  x.strip().replace(' ', ',') if 
                                                x != '' else x)
    
    cleaned_df = processed_df[processed_df[output_column_name] == ''].drop(output_column_name,1).copy(deep=True)    
    processed_df = processed_df[processed_df[output_column_name]!=''][[output_column_name]]    
    processed_df = pd.merge(processed_df, 
                            original_df,
                            left_index=True,
                            right_index = True,
                            how = 'inner')
    
    return cleaned_df, processed_df



def generate_processed_dt():
    return datetime.datetime.now()



def write_to_postgres(df,
                      config, 
                      output = None):
    user = config['postgres_info']['user']
    password = config['postgres_info']['password']
    host = config['postgres_info']['host']
    port = config['postgres_info']['port']
    database = config['postgres_info']['database']
    
    try:
        if_exists = config_object[output]['if_exist']
    except:
        if_exists = 'append'
    
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database))
    df.to_sql(output, engine, if_exists=if_exists, index=False)
    engine.dispose()

def write_to_csv(df,
                 config, 
                 output = None):    
    
    df.to_csv(config[output]['output_path'] + 
              '{}_{}.csv'.format(datetime.datetime.now().strftime('%Y%m%dT%H%M%S'), 
                                 output), 
             index = False)

def write_to_json(df,
                 config, 
                 output = None):    
    
    df.to_json(config[output]['output_path'] + 
              '{}_{}.json'.format(datetime.datetime.now().strftime('%Y%m%dT%H%M%S'), 
                                 output),
              orient = 'table')
    

def output_method(df, config, output):
    output_method = {'csv':write_to_csv, 
                     'json':write_to_json,
                     'postgres':write_to_postgres}
    
    output_method[config[output]['output_type']](df, config, output)

    
def discard_data(df, invalid_data_path, output):
    df.to_csv(invalid_data_path + 
              "DATAERROR_{}_{}.csv".format(output,datetime.datetime.now().strftime('%Y%m%dT%H%M%S')), 
              index = False)
    
    
def fernet_masking(df, columns, key):
    f = Fernet(key)
    for col in columns:
        df[col] = df[col].fillna('')
        df[col] = df[col].map(lambda x: f.encrypt(bytes(x, 'utf-8'))  )
    return df



def timestamp_checker(an_input, if_false = None):
    """
    this checking checks if the input fits a timestamp format
    which is positive number of integer/string
    
    if criteria failed to meet will return if_false value
    """
    
    if (type(an_input) in [float, int]):
        try:
            if an_input > 0:
                return int(an_input)
        except:
            return if_false
    elif type(an_input) == str:
        if an_input.isnumeric():
            if int(an_input) > 0:
                return int(an_input)
            else:
                return if_false
        else:
            return if_false
    else:
        return if_false