import math
import re
import numpy as np


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
            # print(res,pattern)
            if flag_number:
                qc_age = int(res.group(1))
                return qc_age
            else:
                qc_age = 0
                return qc_age
        # else:
        #     return 0


# 分割年龄，为一个的按上面校正，为两个的取平均值


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
get_age('44000')

print(parser_age('smg'))
print(get_age('>23'))