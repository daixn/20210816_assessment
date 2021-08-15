import configparser

def extract_config(file_path = './config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    
    
    config_object = {}
    for element in list(config.keys()):
        config_object[element] = dict(config.items(element))
        
    config_object['execution_process']['output_to_process'] = config_object['execution_process']['output_to_process'].split(',')
    return config_object


