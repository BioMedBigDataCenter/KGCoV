import os
from utils.data_file import filepath
from utils.case_indicators_qc import get_gender
from utils.case_indicators_qc import get_datetime
from utils.case_indicators_qc import get_age
from time import sleep
import re
import numpy as np
import pandas as pd
from utils.mkdir import mkdir
from utils.network import get_csv
from utils.data_file import filepath


# OPEN_LINE data can be downloaded from:
# https://github.com/beoutbreakprepared/nCoV2019/raw/master/latest_data/latestdata.tar.gz

# Other raw data that support the findings of this study are available from the corresponding author upon reasonable request.


ROOT_DIR = os.path.dirname(os.path.abspath('code'))
# INPUT
OPEN_LINE = filepath(ROOT_DIR, 'latestdata.tsv')
GOOGLE_MAP = filepath(ROOT_DIR, 'google_country.tsv')
GOOGLE_LOCATION = filepath(ROOT_DIR, 'google_long_location.tsv')
WIKI = filepath(ROOT_DIR, 'wiki.tsv')
MANUAL = filepath(ROOT_DIR, 'manual.tsv')
ARTICLE_MERGE = filepath(ROOT_DIR, 'article.csv')
TRAVEL = filepath(ROOT_DIR, 'travel.tsv')
CONTACT = filepath(ROOT_DIR, 'contact.tsv')
CLINICAL = filepath(ROOT_DIR, 'clinical.tsv')

# OUTPUT
OUTPUT_DIR = filepath(ROOT_DIR, 'article')
mkdir(OUTPUT_DIR)
ARTICLE_CASE = filepath(OUTPUT_DIR, 'case.tsv')


DROP_COL = ['is_first_case', 'nationality', 'confirmed_date',
            'contact_history', 'travel_history', 'symptoms', 'source_detail', 'country_map']

COLS = {
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

drop_create_columns = ['virus_id', 'PTD', 'date_meta',
                       'long_country_meta', 'gender_meta', 'age_meta', 'source_wiki']


NEED_COL = ['qc_location_id', 'qc_date', 'original_ID', 'age',
            'gender', 'country', 'date', 'url', 'location',
            'data_source', 'new_case_id', 'description', 'qc_country',
            'qc_province', 'qc_location', 'qc_city',
            'qc_continent', 'qc_age', 'qc_gender', 'count', 'rank', 'percentile']

CASE_COL = ['case_id', 'gender', 'qc_gender', 'age', 'qc_age', 'case_reported_date', 'qc_case_reported_date', 'qc_city', 'qc_province',
            'country', 'qc_country', 'location', 'qc_location',
            'qc_continent', 'count', 'rank', 'percentile',
            'data_source', 'description', 'url', 'contactinfo', 'travelinfo', 'clinicalinfo']

CASE_RE = {'new_case_id': 'case_id',
           'qc_date': 'qc_case_reported_date', 'date': 'case_reported_date'}

CASE_DROP = ['qc_location_id', 'original_ID']
CONTINENT_FULL = {
    'AF': 'Africa',
    'AN': 'Antarctica',
    'AS': 'Asia',
    'EU': 'Europe',
    'NA': 'North america',
    'OC': 'Oceania',
    'SA': 'South america',
}


def build():
    df, comments = get_csv(
        'https://download.geonames.org/export/dump/countryInfo.txt',
        sep='\t', header=None,
        comment_prefix='#'
    )
    df.columns = comments.split(
        b'\n')[-1].decode('utf-8').lstrip('# ').split('\t')
    iso_series = df.set_index('Continent').ISO  # type: ignore

    contient_map = pd.Series(CONTINENT_FULL, name='Continent').to_frame() \
        .join(iso_series) \
        .dropna()

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
            'Marocco': 'MA',
            'Hong Kong': 'CN',
            'Taiwan': 'CN',
            'Macao': 'CN',
            'Mainland China': 'CN',
            'Namibia': 'NA',
            'Republic of Congo': 'CD',
            'Trinidad & Tobago': 'TT',
            'Hunan': 'CN',
            'NY': 'US',
            'Bahrein': 'BH',
            'California': 'US',
            'Apulia': 'IT',
            'DC': 'US',
            'VI': 'US',
            'Guinea Bissau': 'GW',
            'LA': 'US',
            'Saint Martin': 'MF',
            'Basilicata': 'IT',
            'Samut Songkhram': 'TH',
            'México': 'MX',
            '\u200eRomania': 'RO',
            'Republic of the Congo': 'CD',
            'Bosnia and Hercegovina': 'BA',
            'CotedIvoire': 'CI',
            'Gujarat': 'IN',
            'Côte d’Ivoire': 'CI',
            'USA? Ohio': 'US',
            'Wales': 'GB',
            'Cote dIvoire': 'CI',
            'Morocoo': 'MA',
            'Bangaldesh': 'BO',
            'Israel': 'IL',
            'Namibia': 'NA',
            'Trinidad': 'TT',
            'St Eustatius': 'BQ',
            'Crimea': 'UA',
            'Czech Repubic': 'CZ',
            'Antigua': 'AG',
            'Georgia (country)': 'GE',
            'Northern Cyprus': 'CY',
            'Virgin Islands, U.S.': 'US',
            'Republic of the Congo': 'CD',
            '中国': 'CN',
            'Israel;': 'IL',
            'Korean': 'KR',
            'Brazilian': 'BR'
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
            'NA': 'Namibia'
        }
    }
    country_map_dict = {
        k.lower(): v for k, v in country_map_dict.items() if not pd.isnull(k)
    }
    print(country_map_dict['na'])
    print(google_map_dict['namibia'])

    # continent

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


    kaggle = pd.read_csv(
        OPEN_LINE,
        sep='\t',
        encoding='utf-8'
    )

    wiki = pd.read_csv(
        WIKI,
        sep='\t',
        encoding='utf-8'
    )

    manual = pd.read_csv(
        MANUAL,
        sep='\t',
        encoding='utf-8'
    )

    article = pd.read_csv(
        ARTICLE_MERGE,
        encoding='utf-8'
    )

    merge_list = [kaggle, wiki, manual]

    df_merge = pd.concat(merge_list, ignore_index=True).reset_index()
    text_new_qc = df_merge.replace({'Marocco': 'Morocco'}) \
        .assign(
            qc_location=lambda x: x.location.map(long_location_dict),
            country_map=lambda x: x.location.map(country_dict),
            country=lambda x: x[['country', 'country_map']].fillna(method='bfill', axis=1)[
                'country'],
            qc_province=lambda x: x.location.map(province_dict),
            qc_city=lambda x: x.location.map(city_dict)
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
            qc_age=lambda x: x.age.apply(get_age).replace(
                {0: np.nan}).astype('Int64'),
            gender=lambda x: x.gender.str.lower().replace({"''": np.nan}),
            qc_gender=lambda x: x.gender.apply(
                get_gender).str.lower().replace({'unknown': np.nan})
    ) \
        .assign(
            location=lambda x: x[['location', 'qc_country']].fillna(
                method='bfill', axis=1)['location'],
            qc_location=lambda x: x[['qc_location', 'qc_country']].fillna(
                method='bfill', axis=1)['qc_location']
    ). \
        set_index("qc_collection_date")['2019-12-01':'2020-12-31']. \
        reset_index() \
        .rename(columns=COLS) \
        .drop(DROP_COL, axis=1)
    text_new_qc.loc[text_new_qc.qc_country ==
                    'Namibia', 'qc_location_id'] = 'NA'

    # drop one records
    text_qc_drop = text_new_qc[text_new_qc.new_case_id != 'C1200002761']

    mrege_list = [text_qc_drop, article]
    case_merge = pd.concat(mrege_list, ignore_index=True)

    case_merge_notna = case_merge[case_merge.qc_location_id.notnull()]

    count = case_merge_notna.groupby(
        ['qc_location_id']).agg({'qc_location_id': ['count']})
    count.columns = count.columns.droplevel(0)
    case_count = pd.merge(case_merge_notna.set_index('qc_location_id'), count, left_index=True, right_index=True, how='left'). \
        drop_duplicates(subset=['new_case_id']). \
        reset_index()

    case_country = case_count.reset_index() \
        .assign(
            rank=lambda x: pd.to_datetime(x.qc_date, errors='coerce').groupby(
                x.qc_location_id).rank(ascending=1, method='dense').astype('Int64'),
            count=lambda x: x['count'].astype('Int64'),
            percentile=lambda x: x['rank'] / x['count'],
            qc_date=lambda x: pd.to_datetime(x.qc_date)
    ). \
        assign(
            qc_date=lambda x: pd.to_datetime(x.qc_date),
            location=lambda x: x[['location', 'qc_country']].fillna(
                method='bfill', axis=1)['location'],
            qc_location=lambda x: x[['qc_location', 'qc_country']].fillna(
                method='bfill', axis=1)['qc_location']
    )[NEED_COL]

    travel = pd.read_csv(
        TRAVEL,
        sep='\t',
        encoding='utf-8',
        usecols=['qc_date', 'from_location',
                 'transport', 'to_location', 'new_case_id']
    )

    contact = pd.read_csv(
        CONTACT,
        sep='\t',
        encoding='utf-8',
        usecols=['qc_date', 'new_contact_case_id', 'new_case_id']
    )

    clinical = pd.read_csv(
        CLINICAL,
        sep='\t',
        encoding='utf-8',
        usecols=['symptom_merge', 'new_case_id']
    )

    # travel merge
    travel_drop = travel.drop_duplicates(subset=['new_case_id'])
    travel_drop.reset_index(drop=True, inplace=True)

    travel_drop.rename(
        columns={'qc_date': 'date', 'new_case_id': 'case_id'}, inplace=True)

    travel_chooose = travel_drop[[
        'date', 'from_location', 'transport', 'to_location']]
    result = []
    travel_list = []

    for ri, row in travel_chooose.iterrows():
        travelinfo = row.to_dict()
        result.append(travelinfo)
        travel_list.append(result)

    travel_data = pd.DataFrame(travel_list)
    travel_info = pd.DataFrame(travel_data.iloc[0])

    travel_info.columns = ['travelinfo']
    travel_caseid = travel_drop[['case_id']]
    travel_merge = travel_caseid.merge(
        travel_info, right_index=True, left_index=True, how='left')

    # concat merge
    contact_drop = contact.dropna(
        subset=['new_contact_case_id']).drop_duplicates(subset=['new_case_id'])
    contact_drop.rename(columns={'qc_date': 'date', 'new_case_id': 'case_id',
                                 'new_contact_case_id': 'contact_case_id'}, inplace=True)
    contact_drop.reset_index(drop=True, inplace=True)

    contact_chooose = contact_drop[['date', 'contact_case_id']]
    result = []
    contact_list = []

    for ri, row in contact_chooose.iterrows():
        contactinfo = row.to_dict()
        result.append(contactinfo)
        contact_list.append(result)

    contact_data = pd.DataFrame(contact_list)
    contact_info = pd.DataFrame(contact_data.iloc[0])

    contact_info.columns = ['contactinfo']

    contact_caseid = contact_drop[['case_id']]

    contact_merge = contact_caseid.merge(
        contact_info, right_index=True, left_index=True, how='left')

    # symptom merge

    clinical_drop = clinical.drop_duplicates(subset='new_case_id')
    clinical_drop.rename(
        columns={'symptom_merge': 'symptom', 'new_case_id': 'case_id'}, inplace=True)
    clinical_drop.reset_index(drop=True, inplace=True)

    clinical_choose = clinical_drop[['symptom']]

    result = []
    clinical_list = []

    for ri, row in clinical_choose.iterrows():
        clinicalinfo = row.to_dict()
        result.append(clinicalinfo)
        clinical_list.append(result)

    clinical_data = pd.DataFrame(clinical_list)
    clinical_info = pd.DataFrame(clinical_data.iloc[0])

    clinical_info.columns = ['clinicalinfo']

    clinical_caseid = clinical_drop[['case_id']]

    clinical_merge = clinical_caseid.merge(
        clinical_info, right_index=True, left_index=True, how='left')

    case_country['data_source'] = case_country['data_source'].replace(
        {'kaggle': 'beoutbreakprepared'})

    case_country.rename(columns=CASE_RE, inplace=True)
    case_drop = case_country.drop(CASE_DROP, axis=1)

    case_travel = case_drop.merge(travel_merge, on='case_id', how='left').drop_duplicates(
        subset=['case_id'], keep='last')
    case_contact = case_travel.merge(
        contact_merge, on='case_id', how='left').drop_duplicates(subset=['case_id'])
    case_clinical = case_contact.merge(
        clinical_merge, on='case_id', how='left').drop_duplicates(subset=['case_id'], keep='last')

    case_adjust = case_clinical[CASE_COL]

    case_adjust.to_csv(
        ARTICLE_CASE,
        sep='\t',
        encoding='utf-8',
        index=False
    )
    print(case_adjust.shape)
    print(case_adjust.columns)
    print(case_adjust.data_source.value_counts())


if __name__ == "__main__":
    build()
