import pandas as pd
import numpy as np
from datetime import datetime
import rootpath
rootpath.append()
from src.qc.gender_qc import get_gender
from src.qc.case_age_qc import get_age
from src.qc.case_date_qc import get_datetime
from src.utils.data_file import filepath

# INPUT
ARTICLE_MERGE = filepath(rootpath.detect(),'data/sync/KGCOV/output/text','article_merge_qc.csv')
TEXT_MERGE_QC = filepath(rootpath.detect(), 'data/sync/KGCOV/output/text','text_merge_qc.tsv')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data')
NODE_CASE = filepath(OUTPUT_DIR,'NODE_Case_new.tsv')

genome_usecols = ['virus_id','qc_location_id','date','qc_date', \
    'age','qc_age','gender','qc_gender','location','country', \
        'qc_country','qc_continent']

need_columns = ['qc_location_id', 'qc_date', 'original_ID', 'age', \
       'gender', 'country', 'date', 'url', 'location', \
       'data_source', 'new_case_id', 'description', 'qc_country', \
        'qc_province','qc_location','qc_city',
       'qc_continent', 'qc_age', 'qc_gender', 'count', 'rank', 'percentile']

def build():
    article = pd.read_csv(
        ARTICLE_MERGE,
        encoding='utf-8'
    )

    text_merge = pd.read_csv(
        TEXT_MERGE_QC,
        sep='\t',
        encoding='utf-8'
    )


    mrege_list = [text_merge,article]
    case_merge = pd.concat(mrege_list,ignore_index=True)
    print('ori',case_merge.shape)
    print(case_merge.columns)

    case_merge_notna = case_merge[case_merge.qc_location_id.notnull()]

    count = case_merge_notna.groupby(
        ['qc_location_id']).agg({'qc_location_id': ['count']})
    count.columns = count.columns.droplevel(0)
    case_count = pd.merge(case_merge_notna.set_index('qc_location_id'), count, left_index=True, right_index=True,how='left'). \
            drop_duplicates(subset=['new_case_id']). \
                reset_index()
    print('now',case_count.shape)


    case_country = case_count.reset_index() \
        .assign(
            rank=lambda x: pd.to_datetime(x.qc_date,errors='coerce').groupby(
                x.qc_location_id).rank(ascending=1, method='dense').astype('Int64'),
            count=lambda x: x['count'].astype('Int64'),
            percentile=lambda x: x['rank'] / x['count'],
            qc_date = lambda x: pd.to_datetime(x.qc_date)
    ). \
        assign(
            qc_date = lambda x: pd.to_datetime(x.qc_date),
            location = lambda x: x[['location','qc_country']].fillna(method='bfill',axis=1)['location'],
            qc_location = lambda x: x[['qc_location','qc_country']].fillna(method='bfill',axis=1)['qc_location']
        )[need_columns]

    print(case_country[case_country.duplicated(subset=['new_case_id'])])
    print(case_country[case_country.duplicated()])
    print(case_country.shape)
    


    case_country.to_csv(
        NODE_CASE,
        sep='\t',
        encoding='utf-8',
        index=False
    )

    print(case_country.shape) 
    print('sucess')

if __name__ == "__main__":
    build()
