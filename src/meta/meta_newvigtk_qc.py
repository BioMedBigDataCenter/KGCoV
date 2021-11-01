# 0714 NODE_Genome
import rootpath 
rootpath.append()
import pandas as pd
import numpy as np
from datetime import datetime
import time
import math
import re
from tqdm import tqdm
from time import sleep
from src.qc.genome_age_qc import get_age
from src.qc.genome_date_qc import get_datetime
from src.qc.gender_qc import get_gender
from src.qc.genome_location_alter import get_genome_location
import math
import re
from src.utils.data_file import filepath
from src.qc.location_qc import get_country
from src.qc.mkdir import mkdir
from src.qc.genome_data_source import get_genome_data_source
  
# INPUT
GOOGLE_MAP = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','google_country.tsv')
CONTENIENT = filepath(rootpath.detect(),'data/sync/KGCOV/output/controlled_vocab','continent_map.tsv')
GOOGLE_LOCATION = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/generated', 'google_long_location.tsv')
VIGTK_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/meta','genome_table.tsv')




# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
NODE_Genome = filepath(OUTPUT_DIR,'NODE_Genome.tsv')


def build():
    drop_columns = ['type','genome_file','length','addition_host','addition_location', \
        'sample_id_provider','sequencing_tech','sample_id_laboratory','assembly_method', \
            'coverage','submitter_lab','country_map']

    columns = {
        'vigtk_id':'virus_id',
        'accession_id':'xref_id',
        'virus_name':'xref_name',
        'submission_date':'release_date',
        'patient_age':'age',
        'collection_date':'date',
        'qc_collection_date':'qc_date',
        'qc_country':'qc_country'
    }


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
            'USA': 'US',
            'UK': 'GB',
            'Korean':'KR',
            'Brazilian':'BR',
            'Namibia':'NAM'
        }
    }
    google_map_dict = {
        k.lower(): v for k, v in google_map_dict.items() if not pd.isnull(k)}

    # 加上洲
    contient_map = pd.read_csv(
        CONTENIENT,
        sep='\t'
    )

    contient_map_dict = dict(zip(contient_map.ISO.str.lower(),contient_map.Continent))

    country_map_dict = {
        **google_map.set_index('short')['text'].to_dict(),
        **google_map.set_index('short')['long'].to_dict(),
        **{
            'NAM':'Namibia'
        }
    }
    country_map_dict = {
        k.lower(): v for k, v in country_map_dict.items() if not pd.isnull(k)
    }

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


    

    # 读入meta数据，用google_map校正国家,pd.to_datetime转换日期为datetime
    meta = pd.read_csv(
        VIGTK_TABLE,
        sep='\t',
        encoding='utf-8'
    ) \
        .dropna(subset=['collection_date']) \
        .assign(
            country = lambda x: x.location.apply(get_country).str.strip()) \
        .replace({'Marocco':'Morocco'}) \
            .assign(
                location = lambda x: x.location.apply(get_genome_location),
                qc_location = lambda x: x.location.map(long_location_dict),
                country_map = lambda x: x.location.map(country_dict),
                country = lambda x: x[['country','country_map']].fillna(method='bfill', axis=1)['country'],
                qc_province = lambda x: x.location.map(province_dict),
                qc_city = lambda x: x.location.map(city_dict),
                qc_location_id=lambda x: x.country.str.strip().str.lower()
                .map(google_map_dict).str.upper(),
                qc_country = lambda x: x.qc_location_id.str.strip().str.lower()
                .map(country_map_dict),
                qc_continent=lambda x: x.qc_location_id.str.strip().str.lower()
                .map(contient_map_dict)
            ) \
        .sort_values(by=['qc_location_id', 'qc_collection_date']).reset_index(drop=True) \
        .assign(
            patient_age=lambda x: x.patient_age.str.lower(),
            qc_age=lambda x: x.patient_age.apply(get_age).replace({0:np.nan}).astype('Int64'),
            gender=lambda x: x.gender.str.lower().replace({"''": np.nan}),
            qc_gender=lambda x: x.gender.apply(get_gender).str.lower() \
                .replace({
                    'unknown':np.NaN
                })
        ) \
        .assign(
            location = lambda x: x[['location','qc_country']].fillna(method='bfill',axis=1)['location'],
            qc_location = lambda x: x[['qc_location','qc_country']].fillna(method='bfill',axis=1)['qc_location'],
            data_source = lambda x: x.accession_id.apply(get_genome_data_source)
        ) \
        .drop(drop_columns,axis=1) \
            .rename(columns=columns)
    meta_drop = meta[meta.data_source=='GISAID'].dropna(subset=['qc_country'])
    print('now',meta_drop.shape)

    meta_na = meta_drop[meta_drop.qc_location_id.isna()]
    meta_notna = meta_drop[meta_drop.qc_location_id.notna()]
    print(meta_na.shape,meta_notna.shape)

    count = meta_notna.groupby(['qc_location_id']).agg({'qc_location_id':['count']})
    count.columns = count.columns.droplevel(0)
    print(count.shape)
    print(count.head())
    meta_merge_count = pd.merge(meta_notna.set_index('qc_location_id'),count,left_index=True, right_index=True,how='left').drop_duplicates(subset=['virus_id'])
    meta_merge_country = meta_merge_count.reset_index() \
        .assign(
            rank = lambda x: pd.to_datetime(x.qc_date).groupby(x.qc_location_id).rank(ascending=1, method='dense').astype('Int64'),
            count = lambda x: x['count'].astype('Int64'),
            percentile = lambda x: x['rank'] / x['count'],
            xref_id = lambda x: x.xref_id.apply(lambda x: x.split('.')[0])
        )

    print('now two',meta_merge_count.shape)
 
    genome_merge = pd.concat([meta_merge_country,meta_na],ignore_index=False). \
        dropna(subset=['host'])
    print(genome_merge.shape)
    genome_merge_host = genome_merge[genome_merge.host.isin(['Human','Homo sapiens',
       'human', 'HUMAN','Hman'])]
    print(genome_merge_host[genome_merge_host.virus_id=='OEAV2702026'])
    print('end',genome_merge_host.shape)
    genome_merge_host.to_csv(
            NODE_Genome,
            sep='\t',
            index=False,
            encoding='utf-8'
        )

    print(genome_merge_host.shape)
    print('sucess')


if __name__ == '__main__':
    build()

