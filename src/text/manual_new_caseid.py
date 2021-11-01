import rootpath
rootpath.append()
import pandas as pd
import numpy as np 
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir
from src.qc.wiki_location_qc import get_wiki_location


# INPUT
SK_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','top5-旅行史结构化（韩国28个病例）.xlsx')
DXY = filepath(rootpath.detect(),'data/sync/KGCOV/raw/downloaded','DXY_line_list_0628.csv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text')
mkdir(OUTPUT_DIR)
MANUAL_NEW = filepath(OUTPUT_DIR,'manual_new_caseid.tsv')


def build():
    columns = {
        'date_meta':'collection_date',
        'long_country_meta':'country',
        'gender_meta':'gender',
        'age_meta':'age',
        'source_wiki':'url',
        'location':'location',
        'text':'text'
        }


    SK_text = pd.read_excel(
            SK_TABLE
        ) \
            .replace({'/':np.nan})
    sk_choose = SK_text[SK_text.PTD.isnull()].iloc[:,2:] \
        .rename(columns=columns) \
        .reset_index(drop=True) \
        .assign(
            original_ID=lambda x: x.index,
            source='manual',
            url = 'https://www.e-epih.org/journal/view.php?doi=10.4178/epih.e2020007',
            location = lambda x: x.apply(lambda x: get_wiki_location(x.location,x.country),axis=1)
        )

    sk_choose['new_case_id'] = pd.Series([
            f'C{str(i+1300000000).zfill(10)}'
            for i in range(sk_choose.shape[0])
        ])

    COLUMNS = {
        'id': 'original_ID',
        'country': 'country', 
        'location': 'location', 
        'reporting date': 'collection_date',
        'age': 'age', 
        'gender': 'gender', 
        'summary': 'text',
        'link': 'url',
        'symptom': 'symptoms',
        'source': 'source_detail'
    }

    dxy = pd.read_csv(
        DXY,
        encoding='utf-8',
        header=1,
        usecols=COLUMNS.keys()
    )

    manual_dxy_one = dxy[dxy.id==200] \
        .rename(columns=COLUMNS) \
        .assign(
        source = 'manual',
        new_case_id = 'C1100000199',
        location = lambda x: x.apply(lambda x: get_wiki_location(x.location,x.country),axis=1)
    )

    manual_dxy_two = dxy[dxy.id==3388] \
        .rename(columns=COLUMNS) \
        .assign(
        source = 'manual',
        new_case_id = 'C1100003387',
        location = lambda x: x.apply(lambda x: get_wiki_location(x.location,x.country),axis=1)
    )

    manual_dxy_three = dxy[dxy.id==3231] \
        .rename(columns=COLUMNS) \
        .assign(
        source = 'manual',
        new_case_id = 'C1100003230',
        location = lambda x: x.apply(lambda x: get_wiki_location(x.location,x.country),axis=1)
    )

    manual_dxy_four = dxy[dxy.id==765] \
        .rename(columns=COLUMNS) \
        .assign(
        source = 'manual',
        new_case_id = 'C1100000764',
        location = lambda x: x.apply(lambda x: get_wiki_location(x.location,x.country),axis=1)
    )

    manual_dxy_five = dxy[dxy.id==3162] \
        .rename(columns=COLUMNS) \
        .assign(
        source = 'manual',
        new_case_id = 'C1100003161',
        location = lambda x: x.apply(lambda x: get_wiki_location(x.location,x.country),axis=1)
    )

    merge_list = [sk_choose,manual_dxy_one,manual_dxy_two,manual_dxy_three,manual_dxy_four,manual_dxy_five]
    manual_new = pd.concat(merge_list,ignore_index=True)

    manual_new.to_csv(
                MANUAL_NEW,
                sep='\t',
                encoding='utf-8',
                index=False
            )
    print(manual_new.shape)
    print('sucess')

if __name__ == '__main__':
    build()
