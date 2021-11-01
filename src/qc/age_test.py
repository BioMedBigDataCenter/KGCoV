import math 
import re 

def parser_age(age):
    for flag_number, pattern in [(True, r'(\d+)s'), (True, r'>(\d+)'), (True, r'<(\d+)'),
                                 (True, r'-(\d+)'), (True, r'\?+(\d+)'), (True,r'(\d+)'), 
                                 (True, r'(\d+)'),(True, r'(^[1-9]\d+).\d+'), (True,r'~(\d+)'), 
                                 (True, r'(\d+)+'), (True, r'～(\d+)'),
                                 (True, r'(\d+)-'), (False, r'0.\d+')]:
        res = re.match(pattern, str(age).strip())
        if res != None:
            if flag_number:
                actual_age = int(res.group(1))
                return actual_age
            else:
                actual_age = 1
                return actual_age

# 分割年龄，为一个的按上面校正，为两个的取平均值


def get_age(age):
    if '月' in str(age):
        qc_age = 0
        return qc_age
    elif 'month' in str(age):
        qc_age = 1
        return qc_age
    elif 'months' in str(age):
        qc_age = 1
        return qc_age
    elif 'weeks' in str(age):
        qc_age = 1
        return qc_age
    elif 'child' in str(age):
        qc_age = 6
        return qc_age
    elif age == '':
        qc_age = 0
        return qc_age
    elif len(str(age)) > 3:
        qc_age = 0
        return qc_age
    else:
        age_seg = str(age).split('-')
        if len(age_seg) == 1:
            qc_age = parser_age(age_seg[0])
            return qc_age
        elif len(age_seg) == 2:
            if age_seg[0] == '':
                qc_age = parser_age(age_seg[1])
            elif age_seg[1] == '':
                qc_age = parser_age(age_seg[0])
            elif age_seg[1] == 'month':
                qc_age = 1
            elif age_seg[1] == '':
                qc_age = parser_age(age_seg[0])
            elif age_seg[0] == '00':
                qc_age = 0
            else:
                try:
                    from_age, to_age = int(age_seg[0]), int(age_seg[1])
                    qc_age = math.ceil((to_age-from_age)/2 + from_age)
                    return qc_age
                except ValueError:
                    print(age)
        else:
            raise ValueError('Wrong date: '+str(age))


def parser_meta_age(age):
    success_flag = False
    for pattern in [r'0(\d+)', r'>(\d+)', r'<(\d+)', r'(\d+)\s.*', r'(\d+),.*', r'(\d+)', r'(\d+).\d+', r"(\d+)'\w",r'\w+\s(\d+)']:
        res = re.match(pattern, str(age).strip())
        if res != None:
            actual_age = round(float(res.group(1)))
            success_flag = True
            return actual_age
    if not success_flag:
        raise ValueError('Wrong age: '+str(age))


# 分割年龄，为1个的按上面校正，为两个的取平均值
def get_meta_age(age):
    if age == '':
        qc_age = 0
        return qc_age
    elif age == 'female' or 'male':
        qc_age = 0
        return qc_age
    elif '月' in age:
        qc_age == 0
        return qc_age
    elif '-' in str(age):
        age_seg = str(age).split('-')
        if len(age_seg) == 1:
            qc_age = parser_meta_age(age_seg[0])
            return qc_age
        elif len(age_seg) == 2:
            if age_seg[1] == 'year old':
                qc_age = round(float(age_seg[0]))
            else:
                from_age, to_age = int(age_seg[0]), int(age_seg[1])
                qc_age = round((to_age-from_age)/2 + from_age)
            return qc_age
    elif '.' in str(age):
        age_split = str(age).split('.')
        if len(age_split) == 1:
            qc_age = parser_meta_age(age_split[0])
        elif len(age_split) == 2:
            qc_age = int(age_split[0])
            return qc_age
    elif ',' in str(age):
        age_split = str(age).split(',')
        if len(age_split) == 1:
            qc_age = parser_meta_age(age_split[0])
        elif len(age_split) == 2:
            qc_age = int(age_split[0])
            return qc_age
    else:
        raise ValueError('Wrong date: '+str(age))
    return qc_age
