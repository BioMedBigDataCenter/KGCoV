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
# COUNTRY = filepath(rootpath.detect(),'data/Local/raw','NODE_Country.tsv')
GOOGLE_MAP = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/generated', 'google_country.tsv')
CONTENIENT = filepath(rootpath.detect(), 'data/sync/KGCOV/output/controlled_vocab', 'continent_map.tsv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
NODE_Location = filepath(OUTPUT_DIR,'NODE_Location(CV).tsv')

def build():
    # Country
    columns = {
        'iso3166':'location_id'
    }



    google_map = pd.read_csv(
        GOOGLE_MAP,
        sep='\t'
    )

    append_dict = {
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
            # '中国':'CN',
            'Israel;':'IL',
            'USA': 'US',
            'UK': 'GB',
            'Korean':'KR',
            'Brazilian':'BR',
            'Namibia':'NAM'
        }
    append_dict_reverse = {v:k for k,v in append_dict.items()}
    print(append_dict_reverse)
    google_map_dict = {
        **google_map.set_index('short')['text'].to_dict(),
        **google_map.set_index('short')['long'].to_dict(),
       **append_dict_reverse
    }
    google_map_dict = {
        k.lower(): v for k, v in google_map_dict.items() if not pd.isnull(k)}

    country = pd.DataFrame(google_map_dict.items())
    country.columns = ['qc_location_id','name']
    country = country.dropna(subset=['qc_location_id']) \
        .assign(
            qc_location_id = lambda x: x.qc_location_id.str.upper()
        )

    print(google_map_dict)

    contient_map = pd.read_csv(
        CONTENIENT,
        sep='\t'
    )

    contient_map_dict = dict(zip(contient_map.ISO.str.lower(),contient_map.Continent))

   

    drop = ['CI','CW','ST','RE','MO','HK','TW']
    country_choose = country[~country.qc_location_id.isin(drop)]

    country_choose[country_choose.qc_location_id=='KOR']['name']='Republic of Korea'

    country_add = country_choose.assign(
        continent = lambda x: x.qc_location_id.str.strip().str.lower().map(contient_map_dict)
    )
    
    country_add.to_csv(
            NODE_Location,
            sep='\t',
            encoding='utf-8',
            index=False
        )

    print(country_add.shape)
    print(country_add.qc_location_id.nunique())
    print('sucess')

if __name__ == '__main__':
    build()

# country_add[country_add.duplicated(subset=['name'])]








