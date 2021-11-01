import json
import pandas as pd
import numpy as np
from tqdm import tqdm
from time import sleep
import rootpath
rootpath.append()
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir

# INPUT
TEXT_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data','NODE_Case_new.tsv')
META_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data','NODE_Genome.tsv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match')
mkdir(OUTPUT_DIR)
MATCH_RESULT = filepath(OUTPUT_DIR,'text_meta_matchall.csv')

columns = ['country', 'gender', 'date','location','province']

def build():
    wiki_ori = pd.read_csv(
        TEXT_TABLE,
        usecols=['qc_date', 'qc_gender', 'qc_country', 'new_case_id', 'qc_age', 'rank','qc_location','qc_province'],
        sep='\t'
        ) \
        .rename(columns={'qc_country': 'country', 'qc_age': 'age',
                        'qc_gender': 'gender', 'qc_date': 'date',
                        'qc_location':'location',
                        'qc_province':'province'
                        }) \
        .set_index('new_case_id')

    wiki = wiki_ori \
        .assign(
            date=lambda x: pd.to_datetime(x.date),
            gender=lambda x: x.gender.map({'male': 1, 'female': 2, 'unknown': 0})
        ).dropna(subset=['gender']) \
        .assign(
            gender=lambda x: x.gender.astype(int)
        )[columns]


    meta_ori = pd.read_csv(
        META_TABLE,
        sep='\t',
        usecols=['qc_date', 'qc_gender', 'qc_country', 'virus_id',
                'qc_age', 'rank','qc_location','qc_province']) \
        .rename(columns={'qc_country': 'country', 'qc_age': 'age',
                        'qc_gender': 'gender', 'qc_date': 'date',
                        'qc_location':'location',
                        'qc_province':'province'
                        }) \
        .set_index('virus_id')

    meta = meta_ori \
        .assign(
            date=lambda x: pd.to_datetime(x.date),
            gender=lambda x: x.gender.map({'male': 1, 'female': 2, 'unknown': 0})
        ).dropna(subset=['gender']) \
        .assign(
            gender=lambda x: x.gender.astype(int)
        )[columns]


    result = []
    hit_count = 0
    for virus_id, meta_row in tqdm(meta.iterrows(), total=meta.shape[0]):
        tm_df = wiki.assign(
            abs_delta_coll_days=lambda x: abs((x.date - meta_row.date).dt.days)
        )
        tm_df = tm_df[(tm_df.abs_delta_coll_days < 4) & (
            tm_df.gender == meta_row.gender) & (tm_df.country == meta_row.country)]
        result.extend([virus_id, x] for x in tm_df.index.tolist())


    result_df = pd.DataFrame(result, columns=('virus_id', 'new_case_id')) \
        .merge(meta_ori.add_suffix('_meta'), left_on='virus_id', right_index=True) \
        .merge(wiki_ori.add_suffix('_wiki'), left_on='new_case_id', right_index=True)
    print(result_df.columns)

    result_df.to_csv(
        MATCH_RESULT,
        index=False,
        encoding='utf-8'
    )
    print(result_df[result_df.new_case_id=='C0020003384'])

if __name__ == '__main__':
    build()



