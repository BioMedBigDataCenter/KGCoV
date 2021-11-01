import pandas as pd 
import numpy as np 


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