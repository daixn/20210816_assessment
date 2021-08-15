import os
import config_reader
import common
from test import test
from telephone_numbers import telephone_numbers
from users import users
from jobs_history import jobs_history
import sys
import pandas as pd
import datetime

inFile = sys.argv[1]
config_object = config_reader.extract_config(inFile)

tasks_ = {'test': test,
          'users': users,
          'telephone_numbers':telephone_numbers,
          'jobs_history': jobs_history}
          
job_status = []    
for output in config_object['execution_process']['output_to_process']:
    job_list = []
    job_list.append(output)
    job_list.append(datetime.datetime.now())
    try:
        tasks_[output](config_object)
        job_list.append(datetime.datetime.now())
        job_list.append('COMPLETED')
        job_list.append('')
    except Exception as e:
        job_list.append(datetime.datetime.now())
        job_list.append('FAILED')
        job_list.append(e)
    job_status.append(job_list)
    
print(job_status)
df = pd.DataFrame(job_status, columns = ['output', 'start_dt', 'end_dt', 'status', 'error'])
print(df)
common.write_to_postgres(df, config_object, 'logging')