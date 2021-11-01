import rootpath
rootpath.append()
from src.utils.data_file import filepath
from src.qc.gender_qc import get_gender
from src.qc.case_date_qc import get_datetime
from src.qc.case_age_qc import get_age
from time import sleep
import numpy as np
import pandas as pd
import re
from src.qc.mkdir import mkdir



# Files can be downloaded from:
# https://dev.biosino.org/seafile/#common/lib/f7864777-4dfe-4760-ae85-4a023c466f56/genome%20annonation/Paper/text/text_data_raw

# INPUT
GOOGLE_MAP = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/generated', 'google_country.tsv')
CONTENIENT = filepath(rootpath.detect(), 'data/sync/KGCOV/output/controlled_vocab', 'continent_map.tsv')
GOOGLE_LOCATION = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/generated', 'google_long_location.tsv')
TEXT_TABLE = filepath(rootpath.detect(), 'data/sync/KGCOV/output/text', 'text_new_merge_all.tsv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/text')
mkdir(OUTPUT_DIR)
TEXT_MERGE_QC = filepath(OUTPUT_DIR, 'text_merge_qc.tsv')

drop_columns = ['is_first_case', 'nationality', 'confirmed_date',
                'contact_history', 'travel_history', 'symptoms','source_detail','country_map']



colunms = {
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

drop_create_columns = ['virus_id','PTD','date_meta','long_country_meta','gender_meta','age_meta','source_wiki']


def build():
    # 导入google_map，用来校正国家
    google_map = pd.read_csv(
        GOOGLE_MAP,
        sep='\t',
        encoding='utf-8'
    )
    google_map_dict = {
        **google_map.set_index('text')['short'].to_dict(),
        **google_map.set_index('long')['short'].to_dict(),
        **{
            'USA': 'US',
            'UK': 'GB',
            'Macau': 'CN',
            'Indian': 'IN',
            'England': 'GB',
            'Scotland': 'GB',
            'Leiden': 'NL',
            'Marocco':'MA',
            'Hong Kong':'CN',
            'Taiwan':'CN',
            'Macao':'CN',
            'Mainland China':'CN',
            'Namibia':'NA',
            'Republic of Congo':'CD',
            'Trinidad & Tobago':'TT',
            'Hunan':'CN',
            'NY':'US',
            'Bahrein':'BH',
            'California':'US',
            'Apulia':'IT',
            'DC':'US',
            'VI':'US',
            'Guinea Bissau':'GW',
            'LA':'US',
            'Saint Martin':'MF',
            'Basilicata':'IT',
            'Samut Songkhram':'TH',
            'México':'MX',
            '\u200eRomania':'RO',
            'Republic of the Congo':'CD',
            'Bosnia and Hercegovina':'BA',
            'CotedIvoire':'CI',
            'Gujarat':'IN',
            'Côte d’Ivoire':'CI',
            'USA? Ohio':'US',
            'Wales':'GB',
            'Cote dIvoire':'CI',
            'Morocoo':'MA',
            'Bangaldesh':'BO',
            'Israel':'IL',
            'Namibia':'NA',
            'Trinidad':'TT',
            'St Eustatius':'BQ',
            'Crimea':'UA',
            'Czech Repubic':'CZ',
            'Antigua':'AG',
            'Georgia (country)':'GE',
            'Northern Cyprus':'CY',
            'Virgin Islands, U.S.':'US',
            'Republic of the Congo':'CD',
            '中国':'CN',
            'Israel;':'IL',
            'Korean':'KR',
            'Brazilian':'BR'
        }
    }
    google_map_dict = {
        k.lower(): v for k, v in google_map_dict.items() if not pd.isnull(k)
    }

    # qc_country 用的map
    country_map_dict = {
        **google_map.set_index('short')['text'].to_dict(),
        **google_map.set_index('short')['long'].to_dict(),
        **{
            'NA':'Namibia'
        }
    }
    country_map_dict = {
        k.lower(): v for k, v in country_map_dict.items() if not pd.isnull(k)
    }
    print(country_map_dict['na'])
    print(google_map_dict['namibia'])

    # continent
    contient_map = pd.read_csv(
        CONTENIENT,
        sep='\t',
        encoding='utf-8'
    )

    contient_map_dict = dict(
        zip(contient_map.ISO.str.lower(), contient_map.Continent)
    )

    # google loong location，correct location and province
    google_long_location = pd.read_csv(
        GOOGLE_LOCATION,
        sep='\t',
        encoding='utf-8')

    long_location_dict = {
        **google_long_location.set_index('location')['location_qc'].to_dict()
    }

    province_dict = {
        **google_long_location.set_index('location')['administrative_area_level_1'].to_dict()
    }

    country_dict = {
        **google_long_location.set_index('location')['country'].to_dict()
    }

    city_dict = {
        **google_long_location.set_index('location')['administrative_area_level_2'].to_dict()
    }
    print(city_dict['Herat,Afghanistan'])

    # 读入text文件，用google_map校正国家qc_country
    text_new_qc = pd.read_csv(
        TEXT_TABLE,
        sep='\t',
        encoding='utf-8'
    ) \
        .replace({'Marocco':'Morocco'}) \
        .assign(
            qc_location = lambda x: x.location.map(long_location_dict),
            country_map = lambda x: x.location.map(country_dict),
            country = lambda x: x[['country','country_map']].fillna(method='bfill', axis=1)['country'],
            qc_province = lambda x: x.location.map(province_dict),
            qc_city = lambda x: x.location.map(city_dict)
        ) \
        .assign(
            qc_location_id=lambda x: x.country.str.strip().str.lower()
            .map(google_map_dict).str.upper(),
            qc_country=lambda x: x.qc_location_id.str.strip().str.lower()
            .map(country_map_dict),
            qc_continent=lambda x: x.qc_location_id.str.strip().str.lower()
            .map(contient_map_dict)
        )\
        .dropna(subset=['collection_date']) \
        .assign(
            qc_collection_date=lambda x: x.collection_date.apply(get_datetime)
        ) \
        .set_index('qc_collection_date').reset_index() \
        .sort_values(by=['qc_location_id', 'qc_collection_date']).reset_index(drop=True) \
        .assign(
            qc_age=lambda x: x.age.apply(get_age).replace({0:np.nan}).astype('Int64'),
            gender=lambda x: x.gender.str.lower().replace({"''": np.nan}),
            qc_gender=lambda x: x.gender.apply(get_gender).str.lower().replace({'unknown':np.nan})
        ) \
         .assign(
            location = lambda x: x[['location','qc_country']].fillna(method='bfill',axis=1)['location'],
            qc_location = lambda x: x[['qc_location','qc_country']].fillna(method='bfill',axis=1)['qc_location']
        ). \
            set_index("qc_collection_date")['2019-12-01':'2020-12-31']. \
            reset_index() \
        .rename(columns=colunms) \
        .drop(drop_columns, axis=1)
    text_new_qc.loc[text_new_qc.qc_country=='Namibia','qc_location_id']='NA'
       
    print(text_new_qc[text_new_qc.location=='Windhoek,Namibia'])

    # 删除south korea一月三十一日48岁男子
    text_qc_drop = text_new_qc[text_new_qc.new_case_id != 'C1200002761']

    print(text_qc_drop.shape)
    print(text_qc_drop.head())
    print(text_qc_drop.columns)


    text_qc_drop.to_csv(
        TEXT_MERGE_QC,
        sep='\t',
        index=False,
        encoding='utf-8'
    )
    print(text_qc_drop.shape)
    print(text_qc_drop.columns)
    print('sucess')
if __name__ == "__main__":
    build()
