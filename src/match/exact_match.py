import pandas as pd
import numpy as np
import rootpath
rootpath.append()
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir

# INPUT
MATCH_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','text_meta_matchall.csv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match')
mkdir(OUTPUT_DIR)
EXACT_MATCH = filepath(OUTPUT_DIR,'exact_matchall_text_meta.csv')

def build():
    meta_wiki = pd.read_csv(
        MATCH_TABLE
    ) \
        .assign(
            age_meta=lambda x: x.age_meta.fillna(value=0).astype('Int64'),
            age_wiki=lambda x: x.age_wiki.fillna(value=0).astype('Int64'),
            date_meta=lambda x: pd.to_datetime(x.date_meta),
            date_wiki=lambda x: pd.to_datetime(x.date_wiki)
        )


    tm_df = meta_wiki.assign(
        abs_delta_coll_days=lambda x: abs((x.date_wiki - x.date_meta).dt.days)
    )
   
    tm_df = tm_df[
        (tm_df.abs_delta_coll_days <= 1) &
        (tm_df.gender_meta == tm_df.gender_wiki) &
        (tm_df.country_meta == tm_df.country_wiki) &
        (tm_df.age_meta == tm_df.age_wiki)&
        ((tm_df['location_meta'] == tm_df['location_wiki'])|(pd.isnull(tm_df['location_meta'])&pd.isnull(tm_df['location_wiki'])))
    ] \
        .drop(columns=['abs_delta_coll_days'])

    tm_df.to_csv(
        EXACT_MATCH,
        index=False,
        encoding='utf-8'
    )
    print('sucess')
    print(tm_df[tm_df.new_case_id=='C0020003384'])
    print(tm_df.shape)
    

if __name__ == '__main__':
    build()
