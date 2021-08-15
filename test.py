import pandas as pd
import os
from common import *
import logging

def __read_files(path):
    list_of_transaction_files = []

    for file in os.listdir(path):
        if file.endswith('.csv'):
            list_of_transaction_files.append(pd.read_csv(path + file))

    transactions_df = pd.concat(list_of_transaction_files)
    return transactions_df


@invalid_info_in_column(columns = ['is_claimed'])
def __clean_is_claimed(df, column ='is_claimed'):
    """
    input will be converted to boolean if it is boolean/'true'/'false'
    remaining input of unidentified string and 
    non-string type will be considered as invalid input    
    """
    def cleaning_logic(an_input):
        if type(an_input) == bool:
            return an_input
        elif type(an_input) != str:
            return None
        elif an_input.lower().strip() == 'true':
            return True
        elif an_input.lower().strip() == 'false':
            return False
        else:
            return None
        
    df[column] = df[column].map(lambda x: cleaning_logic(x))
    return df


@invalid_info_in_column(columns = ['paid_amount'])
def __clean_paid_amount(df, column = 'paid_amount', decimal_places = 2 ):
    def clearning_logic(an_input):
        try:
            return round(float(an_input), decimal_places)
        except:
            return None
        
    df[column] = df[column].map(lambda x: clearning_logic(x))
    return df

@invalid_info_in_column(columns =  ['created_at', 'last_login'])
def __clean_timestamp(df, columns, if_false = None):
    """
    function will try to parse column into timestamp 
    based on format listed in date_format_check list
    list could be further extend for different 
    additional new format    
    """
    def cleaning_logic(an_input):
        date_format_check = ['%Y-%m-%d %H:%M:%S', 
                             '%Y-%m-%dT%H:%M:%S']
        for a_format in date_format_check:
            try:
                return int(datetime.datetime.timestamp(datetime.datetime.strptime(an_input, a_format)))
            except:
                pass
        return an_input
            
    for column in columns:
        df[column] = df[column].map(lambda x: cleaning_logic(x))    
        df[column] = df[column].map(lambda x: timestamp_checker(x, if_false))
    return df

def __generate_unique_id(df, 
                       columns_to_generate_id = ['id', 'created_at', 'last_login'], 
                       output_column = 'unique_id' ):
    """
    function combines all columns provided into a long string and generate hash code from it
    df - dataframe of input
    output_column - name of the desired output hash column
    columns_to_generate_id - list of columns to combine for generating hash column
    
    example: generate_unique_id(df, 'output', ['a', 'b', 'c'])
    is equivalent to creating a new column 'output' in dataframe(df)
    by creating a hash value based on columns a,b,c
    """
    df[output_column] = df[columns_to_generate_id].apply(lambda x: ''.join(x.astype(str)),1)
    df[output_column] = df[output_column].map(lambda x: hash(x))
    return df

def test(config):    
    logging.basicConfig(filename="{}{}_test.log".format(config['test']['logging_path'],
                                                        datetime.datetime.now().strftime('%Y%m%dT%H%M%S')), 
                        level=logging.DEBUG,
                        force=True)
    logger=logging.getLogger() 
    try:
        transactions_df = __read_files(config['test']['raw_files'])
        logger.info("Transaction files read successfully") 

        backup_transaction_df = transactions_df.copy(deep=True)
        logger.info("Transaction files deep copied successfully") 

        transactions_df = __clean_is_claimed(transactions_df)
        logger.info("IS CLAIM column cleaned successfully") 

        transactions_df = __clean_paid_amount(transactions_df)
        logger.info("PAID AMOUNT column cleaned successfully") 

        transactions_df = __clean_timestamp(transactions_df, ['created_at', 'last_login'])
        logger.info("TIMESTAMP cleaned successfully")     

        transactions_df = __generate_unique_id(transactions_df)
        logger.info("UNIQUE ID generated successfully")

        transactions_df['address'] = transactions_df['address'].map(lambda x: clean_crlf(x))
        logger.info("Line break and Carriage Return cleaned successfully")

        clean_df, rejected_df = clean_error_data(backup_transaction_df, transactions_df)    
        discard_data(rejected_df, config['test']['invalid_data'], 'test')
        logger.info("REJECTED data moved to INVALID folder successfully")    

        clean_df['created_at'] = clean_df['created_at'].map(lambda x: datetime.datetime.fromtimestamp(x))
        logger.info("CREATED_AT cleaned successfully")    

        clean_df['last_login'] = clean_df['last_login'].map(lambda x: datetime.datetime.fromtimestamp(x))
        logger.info("LAST LOGIN cleaned successfully")

        clean_df['processed_dt'] = generate_processed_dt()
        logger.info("PROCESSED DT generated successfully")

        output_method(clean_df, config, 'test')
        logger.info("OUTPUT generated successfully")
        logger.info("###PROCESS COMPLETED###")
        logging.shutdown()
    except Exception as E:
        logger.critical("Process failed: Error message {}".format(E)) 
        logging.shutdown()