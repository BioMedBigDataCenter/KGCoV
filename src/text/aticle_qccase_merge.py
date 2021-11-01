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
GOOGLE_MAP = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/generated', 'google_country.tsv')
CONTENIENT = filepath(rootpath.detect(), 'data/sync/KGCOV/output/controlled_vocab', 'continent_map.tsv')
ARTICLE = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','article_ncov_merge.csv')
MANUAL = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','文献中的病例信息-汇总.xlsx')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text')
ARTICLE_MERGE = filepath(OUTPUT_DIR,'article_merge_qc.csv')

usecols_article = ['PMID','age','country','onset date','gender']
columns_article = {
    'onset date':'date',
    'PMID':'pmid'
}
usecols_manual = ['PMID','patient_id','date','gender','age','country']
columns_manual = {
    'PMID':'pmid'
}

def build():
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
        **google_map.set_index('short')['long'].to_dict()
    }
    country_map_dict = {
        k.lower(): v for k, v in country_map_dict.items() if not pd.isnull(k)
    }


    # continent
    contient_map = pd.read_csv(
        CONTENIENT,
        sep='\t',
        encoding='utf-8'
    )

    contient_map_dict = dict(
        zip(contient_map.ISO.str.lower(), contient_map.Continent)
    )

    article = pd.read_csv(
        ARTICLE,
        encoding='unicode-escape',
        usecols=usecols_article
    ) \
    .rename(columns=columns_article)

    manual = pd.read_excel(
        MANUAL,
        usecols=usecols_manual
    ) \
    .rename(columns=columns_manual)

    article_new = article.dropna(subset=['date']) \
        .assign(
            qc_gender = lambda x: x.gender.apply(get_gender),
            qc_age = lambda x: x.age.apply(get_age).astype('Int64'),
            qc_date = lambda x: x.date.apply(get_datetime)
        ) \
        .reset_index()

    article_new['patient_id'] = pd.Series([
        f'{str(i).zfill(2)}'
        for i in range(article_new.shape[0])
    ])

    article_new = article_new.assign(
        new_case_id = lambda x: 'C' + x.pmid.astype('str') + x.patient_id
    )

    manual_new = manual.replace({'-':np.nan,'           —':np.nan,' -':np.nan,'        —':np.nan,'          —':np.nan,'         —':np.nan,'~':'-'}) \
        .dropna(subset=['date'])
    manual_new[manual_new.age=='1.7s']=1
    manual_new[manual_new.age=='infant']=1
    manual_new[manual_new.gender==1]=np.nan

    manual_age = manual_new.assign(
        qc_gender = lambda x: x.gender.apply(get_gender),
        qc_age = lambda x: x.age.apply(get_age),
        qc_date = lambda x: pd.to_datetime(x.date, errors='coerce')
    )

    manual_date = manual_age.dropna(subset=['qc_date']).reset_index() \
    .assign(
        pmid = lambda x: x.pmid.astype('int64'),
        patient_id = lambda x: x.patient_id.astype('int64')
    )

    manual_id = manual_date.assign(
        patientid = lambda x: x.patient_id.astype('str').str.zfill(2),
        new_case_id = lambda x: 'C' + x.pmid.astype('str') + x.patientid.astype('str')
    )

    manual_id.drop(['index','patientid','patient_id'],axis=1,inplace=True)

    article_new.drop(['index','patient_id'],axis=1,inplace=True)

    article = pd.concat([manual_id,article_new],ignore_index=True)
    try:
        article_replace = article.replace({'中国':'China'})
    except TypeError:
        article_replace = article
 

    article_new = article_replace.assign(
        qc_location_id=lambda x: x.country.str.strip().str.lower()
        .map(google_map_dict).str.upper(),
        qc_country=lambda x: x.qc_location_id.str.strip().str.lower()
        .map(country_map_dict),
        qc_continent=lambda x: x.qc_location_id.str.strip().str.lower()
        .map(contient_map_dict),
        data_source = 'manual',
        url = lambda x: 'https://pubmed.ncbi.nlm.nih.gov/' + x.pmid.astype('str')
    )


    article_drop = article_new.drop(['pmid'],axis=1) \
        .drop_duplicates(subset=['new_case_id'],keep='first')

    article_drop.to_csv(
        ARTICLE_MERGE,
        index=False,
        encoding='utf-8'
    )

if __name__ == "__main__":
    build()




