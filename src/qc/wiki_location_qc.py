import numpy as np
import pandas as pd


def get_wiki_location(location,country):
#     print(city,province)
    if pd.isnull(location):
        location_qc = None
        return location_qc
        print('case 1',location)
    elif pd.notnull(location):
        if pd.isnull(country):
            location_qc = location
            return location_qc
            print('case 2',location)
        elif pd.notnull(country):
            location_qc = f'{location},{country}'
            return location_qc
            print('case 3',location)
    else:
        print('not found',location,country)
