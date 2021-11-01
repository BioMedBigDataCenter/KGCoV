import rootpath
rootpath.append()

import pandas as pd
import numpy as np
from src.qc.new_case_id import split_join
from src.utils.data_file import filepath
from src.qc.gender_qc import get_gender
from src.qc.case_date_qc import get_datetime
from src.qc.case_age_qc import get_age
from src.qc.qc_city import parser_city
from src.qc.kaggle_location_qc import get_kaggle_location
from time import sleep
import re
from src.qc.mkdir import mkdir


# INPUT
KAGGLE = filepath(rootpath.detect(),'data/sync/KGCOV/raw/downloaded','nCoV2019_open_line_1116.csv')
GOOGLE_MAP = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/generated', 'google_country.tsv')
CONTENIENT = filepath(rootpath.detect(), 'data/sync/KGCOV/output/controlled_vocab', 'continent_map.tsv')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/target/text')
mkdir(OUTPUT_DIR)
KAGGLE_NEW = filepath(OUTPUT_DIR,'kaggle_new_caseid.tsv')


def build():
    COLUMNS = {
            'ID': 'original_ID', 
            'country': 'country', 
            'province': 'province',
            'city': 'city',
            'date_confirmation': 'collection_date',
            'travel_history_location': 'travel_history',
            'symptoms': 'symptoms',
            'age': 'age', 
            'sex': 'gender', 
            'source': 'url'
        }

    DROP_COLUMNS = ['travel_history', 'symptoms']

    QC_COLUMNS = {
        'CID': 'case_id',
        'collection_date': 'date',
        'qc_collection_date': 'qc_date',
        'gender': 'gender',
        'qc_gender': 'qc_gender',
        'age': 'age',
        'qc_age': 'qc_age',
        'country': 'country',
        'text_source': 'data_source',
        'province': 'location',
        'rank': 'rank',
        'text': 'description',
        'source': 'data_source'
    }

  
    kaggle = pd.read_csv(
        KAGGLE,
        encoding='utf-8',
        usecols=COLUMNS.keys(),
        dtype=str
    ) \
        .rename(columns=COLUMNS) \
        .assign(
                qc_city = lambda x: x.city.apply(parser_city),
                location = lambda x: x.apply(lambda x: get_kaggle_location(x.qc_city,x.province,x.country),axis=1),
                source='kaggle',
                new_case_id = lambda x: x.original_ID.apply(split_join)
            ) \
            .drop(
                columns=['province', 'city','qc_city']
            )
    kaggle.url[kaggle.new_case_id=='C0000121494'] = 'https://www.lefigaro.fr/sciences/coronavirus-desormais-onze-cas-confirmes-en-france-20200209'


    kaggle.to_csv(
        KAGGLE_NEW,
        sep='\t',
        encoding='utf-8',
        index=False
    )
    print(kaggle[kaggle.new_case_id=='C0000121494'])
    print('sucess')

if __name__ == '__main__':
    build()






