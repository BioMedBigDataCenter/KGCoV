import re
import numpy as np
import pandas as pd

def parser_city(city):
    if pd.isnull(city):
        qc_city = None
        return qc_city
    else:
        pattern = r'(^\d+$|\d+\+|\$\d+.*|\d+\-\d+|.*de Mayo.*|.*de mayo.*|\-|.*de julio.*)'
        res = re.match(pattern,city)
        if res != None:
            print('case 1',city)
            qc_city = None
            return qc_city
        else:
            print('case 2',city)
            qc_city = city
            return qc_city