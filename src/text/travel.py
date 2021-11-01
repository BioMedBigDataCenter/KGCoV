import pandas as pd
import numpy as np
from tqdm.notebook import tqdm
import os
from datetime import datetime
import time
import rootpath 
rootpath.append()
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir


# INPUT
TRAVEL = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','旅行史结构化-0630.xlsx')
TOP5 = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','top5-二次审编完成.csv')
TRAVEL_MAP = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','travel_map.xlsx')
GOOGLE_MAP = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/generated', 'google_country.tsv')
CASEID_DICT = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','caseid_dict.tsv')
WIKI_TRAVEL = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','Wiki-旅行史结构化合并.xlsx')
WIKI_MANUAL = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','Wiki-数据清洗合并.xlsx')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
TRAVELINFO = filepath(OUTPUT_DIR,'LINK_TRAVEL_CaseTravelInfo.tsv')

columns = {
    'CID':'case_id',
    'travel_text':'description',
    '什么时间':'qc_date',
    '从哪':'from_location',
    '交通工具':'transport',
    '航班/车次':'tran_number',
    '到哪':'to_location'
}

def build():
    travel_top5 = pd.read_excel(
        TRAVEL
    )

    url_top5 = pd.read_csv(
        TOP5,
        usecols=['PTD','source_wiki'],
        encoding='GB2312'
    ) \
        .assign(
            case_id = lambda x: x.PTD.replace(r'PT.',r'C',regex=True),
            url = lambda x: x.source_wiki
        )[['case_id','url']]

    travel_wiki = pd.read_excel(
        WIKI_TRAVEL
    )

    url_wiki = pd.read_excel(
        WIKI_MANUAL,
        usecols=['PTD','source_wiki']
    ) \
    .assign(
        case_id = lambda x: x.PTD.replace(r'PT.',r'C',regex=True),
        url = lambda x: x.source_wiki
    )[['case_id','url']]


    travel_map = pd.read_excel(
        TRAVEL_MAP
    )

    google_map = pd.read_csv(
        GOOGLE_MAP,
        sep='\t',
        encoding = 'utf-8'
    )

    travel_dict = {
        **travel_map.set_index('location')['country'].to_dict()
    }
    travel_dict = {
        k.lower():v for k,v in travel_dict.items() if not pd.isnull(k)
    }

    google_map_dict = {
        **google_map.set_index('text')['short'].to_dict(),
        **google_map.set_index('long')['short'].to_dict(),
        **{
            'USA': 'US',
            'UK': 'GB',
            'Macau': 'MO',
            'Indian': 'IN',
            'England': 'GB',
            'Scotland': 'GB',
            'Leiden': 'NL'
        }
    }
    google_map_dict = {
        k.lower(): v for k, v in google_map_dict.items() if not pd.isnull(k)
    }

    caseid = pd.read_csv(
    CASEID_DICT,
    sep='\t',
    encoding='utf-8'
    )

    caseid_dict = {
        **caseid.set_index('case_id')['new_case_id'].to_dict()
    }


    travel_merge_top5 = travel_top5.rename(columns=columns) \
        .merge(url_top5,on='case_id',how='left') \
        .drop_duplicates() \
        .replace({
            '/':np.nan
            }) \
        .dropna(subset=['qc_date']) \
        .assign(
            text_patient=lambda x: x.text_patient.str.strip(),
            text_patient_en=lambda x: x.text_patient_en.str.strip(),
            description=lambda x: x.description.str.strip(),
            travel_text_en=lambda x: x.travel_text_en.str.strip(),
            qc_date=lambda x: pd.to_datetime(x.qc_date),
            from_country = lambda x: x.from_location.str.lower().map(travel_dict),
            to_country = lambda x: x.to_location.str.lower().map(travel_dict),
            qc_from_location_id = lambda x: x.from_country.str.lower().map(google_map_dict),
            qc_to_location_id = lambda x: x.to_country.str.lower().map(google_map_dict),
            new_case_id = lambda x: x.case_id.map(caseid_dict)
        ) 

    travel_merge_wiki = travel_wiki.assign(
        case_id = lambda x: x.PTD.replace(r'PT.',r'C',regex=True)
        )\
        .rename(columns=columns) \
        .replace({
            '/':np.nan
            }) \
        .drop_duplicates() \
        .dropna(subset=['qc_date']) \
        .assign(
            text_patient=lambda x: x.text_patient.str.strip(),
            description=lambda x: x.description.str.strip(),
            travel_text_en=lambda x: x.travel_text_en.str.strip(),
            qc_date=lambda x: pd.to_datetime(x.qc_date,errors='coerce'),
            from_country = lambda x: x.from_location.str.lower().str.strip().map(travel_dict),
            to_country = lambda x: x.to_location.str.lower().str.strip().map(travel_dict),
            qc_from_location_id = lambda x: x.from_country.str.strip().str.lower().map(google_map_dict),
            qc_to_location_id = lambda x: x.to_country.str.strip().str.lower().map(google_map_dict),
            new_case_id = lambda x: x.case_id.map(caseid_dict)
        ) \
        .merge(url_wiki,on='case_id',how='left') \
        .drop_duplicates() \
        .dropna(subset=['virus_id','url','new_case_id']) \
        .drop(['PTD','Column11'],axis=1)
        
    

    concat_list = [travel_merge_top5,travel_merge_wiki]

    total_columns = {'from_country':'qc_from_country','to_country':'qc_to_country'}
    travel_merge = pd.concat(concat_list,ignore_index=True)
    travel_new = travel_merge.rename(
        columns = total_columns
        ) \
        .drop(['case_id'],axis=1) \
        .dropna(subset=['new_case_id']) \
        .dropna(subset=['from_location','transport','tran_number','to_location'],how='all')

    travel_new.to_csv(
            TRAVELINFO,
            sep='\t',
            index=False
        )

    print(travel_new.shape) #TODO:(184, 16),原来是(174, 16)

if __name__ == '__main__':
    build()



# travel.rename(columns=columns,inplace=True)
# travel_merge = pd.merge(travel,virus_id,on='xref_id',how='left')
# travel_merge.drop('xref_id',axis=1,inplace=True)
# travel_merge_drop = travel_merge.drop_duplicates()
# travel_merge_drop.shape
