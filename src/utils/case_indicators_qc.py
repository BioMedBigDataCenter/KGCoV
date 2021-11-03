import numpy as np   
from datetime import datetime
import time
import re
import pandas as pd 
import math

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

def parse_date(collection_date):
    success_flag = False
    if '.' in collection_date:
        pattern = r'(\d+)\.(\d+)\.2020'
        res = re.match(pattern, str(collection_date).strip(), re.S)
        if res != None:
            date = datetime(2020, int(res.group(2)), int(res.group(1)))
            success_flag = True
            return date 
            print('case 5',date)

    elif '-' in collection_date:
        for pattern in [r'2020\-(\d+)\-(\d+)\s\d+\:',r'2020\-(\d+)\-(\d+)']:
            res = re.match(pattern, str(collection_date).strip(), re.S)
            if res != None:
                date = datetime(2020, int(res.group(1)), int(res.group(2)))
                success_flag = True
                return date 
                print('case 6',date)

    elif '/' in collection_date:
        for pattern in [r'2020\/(\d+)\/(\d+)', r'(\d+)\/(\d+)\/2020']:
            res = re.match(pattern, str(collection_date).strip(), re.S)
            if res != None:
                date = datetime(2020, int(res.group(1)), int(res.group(2)))
                success_flag = True
                return date 
                print('case 7',date)
                break
    
    if not success_flag:
        raise ValueError('Wrong date: '+str(collection_date))
        return date
        print('case 7',collection_date)


def get_datetime(case_date):
    if '2020' not in case_date:
        actual_date = pd.to_datetime(case_date)
        return actual_date
        print('case 1',actual_date)
    casedate_seg = str(case_date).strip().split('-')
    if len(casedate_seg) == 1:
        actual_date = parse_date(casedate_seg[0])
        return actual_date
        print('case 2',actual_date)
    elif len(casedate_seg) == 2:
        fromdate, todate = parse_date(casedate_seg[0]), parse_date(casedate_seg[1])
        actual_date = (todate-fromdate)/2 + fromdate
        return actual_date
        print('case 3',actual_date)
    elif len(casedate_seg) == 3:
        actual_date = parse_date(case_date)
        return actual_date
        print('case 4',actual_date)
    else:
        actual_date = None
        return actual_date
        raise ValueError('Wrong date: '+str(case_date))

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
            else:
                try:
                    from_age, to_age = int(age_seg[0]), int(age_seg[1])
                    qc_age = math.ceil((to_age-from_age)/2 + from_age)
                    return qc_age
                except ValueError:
                    raise ValueError('Wrong age1: '+str(age))
    elif '.' in str(age):
        print('case 2',age)
        age_seg = str(age).split('.')
        if len(age_seg) == 1:
            qc_age = parser_age(age_seg[0])
        elif len(age_seg) == 2:
            qc_age = math.ceil(float(age))
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