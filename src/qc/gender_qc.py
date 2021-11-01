import numpy as np   

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