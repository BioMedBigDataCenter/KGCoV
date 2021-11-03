import math
import re
import numpy as np
from datetime import datetime
import time
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

def get_gender(gender):
    if gender == 'male':
        qc_gender = 'male'
        return qc_gender
    elif gender == 'female':
        qc_gender = 'female'
        return qc_gender
    elif gender == '':
        qc_gender = np.nan
        return qc_gender
    elif gender == 'u':
        qc_gender = np.nan
        return qc_gender
    elif gender == 'unknwon':
        qc_gender = np.nan
        return qc_gender
    elif gender == 'woman':
        qc_gender = 'female'
        return qc_gender

def get_province(location):
    try:
        split = location.split('/')
    except AttributeError:
        province = 'unknown'
        return province
    try:
        province = split[2]
        return province
    except IndexError:
        province = 'unknown'
        return province

def get_country(location):
    try:
        split = location.split('/')
    except AttributeError:
        country = np.nan
        return country
    try:
        country = split[1]
        return country
    except IndexError:
        country = np.nan
        return country

def parser_age(age):
    for flag_number, pattern in [(True, r'(\d+)s'), (True, r'>(\d+)'), (True, r'<(\d+)'),
                                 (True, r'^-(\d+)'), (True,r'\?+(\d+)'), (True, r'(\d+)'),
                                 (True, r'(\d+)'), (True,r'(^[1-9]\d+).\d+'), (True, r'~(\d+)'),
                                 (True, r'(\d+)+'), (True,r'～(\d+)'), (True, r'0(\d+)'),
                                 (True, r'>(\d+)'), (True,r'(\d+)\s.*'), (True, r'(\d+),.*'),
                                 (True, r'(\d+){1,3}'), (True,r'(\d+).\d+'), (True, r"(\d+)'\w"),
                                 (True, r'\w+\s(\d+)'), (True, r'(\d+)-'), (False, r'^\d{4}')]:
        res = re.match(pattern, str(age).strip())
        if res != None:
            print('case 6',age)
            if flag_number:
                qc_age = int(res.group(1))
                return qc_age
            else:
                qc_age = 0
                return qc_age



def get_age(age):
    if '-' in str(age):
        print('case 1',age)
        age_seg = str(age).split('-')
        if len(age_seg) == 1:
            qc_age = parser_age(age_seg[0])
            return qc_age
        elif len(age_seg) == 2:
            if age_seg[0] == '':
                qc_age = parser_age(age_seg[1])
                return qc_age
            elif age_seg[1] == '':
                qc_age = parser_age(age_seg[0])
                return qc_age
            elif age_seg[1] == '':
                qc_age = parser_age(age_seg[0])
                return qc_age
            elif age_seg[0] == '00':
                qc_age = 0
                return qc_age
            elif age_seg[1] == 'year old':
                qc_age = age_seg[0]
                return qc_age
            elif age_seg[1] == 'month':
                qc_age = 1
                return qc_age
            elif age_seg[1] == 'Sep':
                qc_age = 0
                return qc_age
            elif 'age' in age_seg[1]:
                age_seg[1] = age_seg[1].split(' ')[0]
                from_age, to_age = int(age_seg[0]), int(age_seg[1])
                qc_age = math.ceil((to_age-from_age)/2 + from_age)
                if len(str(qc_age)) > 3:
                    qc_age = None
                    return qc_age
                elif len(str(qc_age)) < 4:
                    return qc_age
            else:
                try:
                    from_age, to_age = int(age_seg[0]), int(age_seg[1])
                    qc_age = math.ceil((to_age-from_age)/2 + from_age)
                    if len(str(qc_age)) > 3:
                        qc_age = None
                        return qc_age
                    elif len(str(qc_age)) < 4:
                        return qc_age
                except ValueError:
                    raise ValueError('Wrong age1: '+str(age))
    elif '.' in str(age):
        print('case 2',age)
        age_seg = str(age).split('.')
        if len(age_seg) == 1:
            qc_age = parser_age(age_seg[0])
        elif len(age_seg) == 2:
            try:
                qc_age = math.ceil(float(age))
                return qc_age
            except ValueError:
                if 'month' in age:
                    qc_age = 1
                    return qc_age
    elif ',' in str(age):
        print('case 3',age)
        age_seg = str(age).split(',')
        if len(age_seg) == 1:
            qc_age = parser_age(age_seg[0])
        elif len(age_seg) == 2:
            if len(age_seg[0])>4:
                qc_age = int(age_seg[0][:2].strip())
                return qc_age
            else:
                qc_age = int(age_seg[0])
                return qc_age
    elif 'month' in str(age):
        print('case 4',age)
        if 'years' in str(age):
            qc_age = str(age)[:2].strip()
            return qc_age
        elif 'td="td"' in str(age):
            return None
        elif len(str(age))>9:
            qc_age = int(str(age)[:2].strip())
            return qc_age
        else:
            return 1
    elif len(str(age)) > 3:
        pattern = r'(\d{4,5})'
        res = re.match(pattern,str(age))
        if res != None:
            return np.nan
    else:
        try:
            qc_age = parser_age(age)
            print('case 5',age)
            return qc_age
        except AttributeError:
            return 0
            raise ValueError('Wrong age2: '+str(age))

continents = ['Eurasia',
 'Europe',
 'North-America',
 'Afirica',
 'South America',
 'North America',
 'Asian',
 'Central America',
 'Asia',
 'North america',
 'Europa',
 'Oceania',
 'America',
 'Africa',
 'Noth America',
 'South American',
 '']

def get_genome_location(location):
    try:
        location_split = location.split('/')[::-1]
        location_drop = [loc.strip() for loc in location_split if loc.strip() not in continents]
        if len(location_drop) != 1:
            location_qc = ','.join(location_drop)
        elif len(location_drop) == 1:
            location_qc = None
        return location_qc
    except AttributeError:
        location_qc = None
        return location_qc

def get_genome_data_source(xref_id):
    if 'EPI' in xref_id:
        data_source = 'GISAID'
        return data_source
    else:
        data_source = 'GenBank'
        return data_source
