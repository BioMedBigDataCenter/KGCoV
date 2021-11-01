from datetime import datetime
import time
import re
import pandas as pd 



def get_datetime(collection_date):
    if '-' in collection_date:
        res = re.match(r'21\-(\d+)\-(\d+)',str(collection_date).strip(), re.S)
        if res != None:
            actual_date = datetime(2021, int(res.group(1)), int(res.group(2)))
            success_flag = True
            return actual_date 
            print('case 1',actual_date,collection_date)
        else:
            actual_date = pd.to_datetime(collection_date)
            print('case 2',actual_date,collection_date)
            return actual_date
    elif 'Emma' in collection_date:
        actual_date = None
        print('case 3',actual_date,collection_date)
        return actual_date
            
print(get_datetime('21-03-26'))
print(get_datetime(',,Emma Swindells'))

