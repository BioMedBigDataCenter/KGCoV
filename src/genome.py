
import os
import pandas as pd
import numpy as np
from datetime import datetime
import time
import math
import re
from tqdm import tqdm
from time import sleep
from utils.genome_indicators_qc import get_age
from utils.genome_indicators_qc import get_datetime
from utils.genome_indicators_qc import get_gender
import math
import re
from utils.data_file import filepath
from utils.genome_indicators_qc import get_country
from utils.mkdir import mkdir
from utils.genome_indicators_qc import get_genome_location
from utils.genome_indicators_qc import get_genome_data_source
from utils.network import get_csv

# ALL INPUT raw data that support the findings of this study are available from the corresponding author upon reasonable request.

ROOT_DIR = os.path.dirname(os.path.abspath('code'))
# INPUT
META = filepath(ROOT_DIR, 'genome_metadata.txt')
GOOGLE_MAP = filepath(ROOT_DIR, 'google_country.tsv')
GOOGLE_LOCATION = filepath(ROOT_DIR, 'google_long_location.tsv')


# OUTPUT
OUTPUT_DIR = filepath(ROOT_DIR, 'article')
mkdir(OUTPUT_DIR)
ARTICLE_GENOME = filepath(OUTPUT_DIR, 'genome.tsv')

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
    genome_new = open(META, 'r', encoding='utf-8')
    titles = genome_new.readline().split('\t')

    lines = genome_new.readlines()[1:]

    genome_result = {}
    for title in titles:
        genome_result[title.strip()] = []
    print(genome_result)

    for line in lines:
        data = line.split('\t')
        for count in range(len(titles)):
            genome_result[titles[count].strip()].append(data[count])
    genome_df = pd.DataFrame(genome_result)
    genome_table = genome_df.assign(
        qc_collection_date=lambda x: x.collection_date.apply(get_datetime)
    ) \
        .dropna(subset=['qc_collection_date']) \
        .set_index('qc_collection_date')['2019-11-01':'2020-11-30'].reset_index()

    # qc
    drop_columns = ['type', 'genome_file', 'length', 'addition_host', 'addition_location',
                    'sample_id_provider', 'sequencing_tech', 'sample_id_laboratory', 'assembly_method',
                    'coverage', 'submitter_lab', 'country_map']

    columns = {
        'vigtk_id': 'virus_id',
        'accession_id': 'xref_id',
        'virus_name': 'xref_name',
        'submission_date': 'release_date',
        'patient_age': 'age',
        'collection_date': 'date',
        'qc_collection_date': 'qc_date',
        'qc_country': 'qc_country'
    }

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
            'USA': 'US',
            'UK': 'GB',
            'Korean': 'KR',
            'Brazilian': 'BR',
            'Namibia': 'NAM'
        }
    }
    google_map_dict = {
        k.lower(): v for k, v in google_map_dict.items() if not pd.isnull(k)}

    contient_map_dict = dict(
        zip(contient_map.ISO.str.lower(), contient_map.Continent))

    country_map_dict = {
        **google_map.set_index('short')['text'].to_dict(),
        **google_map.set_index('short')['long'].to_dict(),
        **{
            'NAM': 'Namibia'
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

    meta = genome_table \
        .dropna(subset=['collection_date']) \
        .assign(
            country=lambda x: x.location.apply(get_country).str.strip()) \
        .replace({'Marocco': 'Morocco'}) \
        .assign(
            location=lambda x: x.location.apply(get_genome_location),
            qc_location=lambda x: x.location.map(long_location_dict),
            country_map=lambda x: x.location.map(country_dict),
            country=lambda x: x[['country', 'country_map']].fillna(method='bfill', axis=1)[
                'country'],
            qc_province=lambda x: x.location.map(province_dict),
            qc_city=lambda x: x.location.map(city_dict),
            qc_location_id=lambda x: x.country.str.strip().str.lower()
            .map(google_map_dict).str.upper(),
            qc_country=lambda x: x.qc_location_id.str.strip().str.lower()
            .map(country_map_dict),
            qc_continent=lambda x: x.qc_location_id.str.strip().str.lower()
            .map(contient_map_dict)
        ) \
        .sort_values(by=['qc_location_id', 'qc_collection_date']).reset_index(drop=True) \
        .assign(
            patient_age=lambda x: x.patient_age.str.lower(),
            qc_age=lambda x: x.patient_age.apply(
                get_age).replace({0: np.nan}).astype('Int64'),
            gender=lambda x: x.gender.str.lower().replace({"''": np.nan}),
            qc_gender=lambda x: x.gender.apply(get_gender).str.lower()
            .replace({
                'unknown': np.NaN
            })
        ) \
        .assign(
            location=lambda x: x[['location', 'qc_country']].fillna(
                method='bfill', axis=1)['location'],
            qc_location=lambda x: x[['qc_location', 'qc_country']].fillna(
                method='bfill', axis=1)['qc_location'],
            data_source=lambda x: x.accession_id.apply(get_genome_data_source)
        ) \
        .drop(drop_columns, axis=1) \
        .rename(columns=columns)
    meta_drop = meta[meta.data_source == 'GISAID'].dropna(subset=[
                                                          'qc_country'])

    meta_na = meta_drop[meta_drop.qc_location_id.isna()]
    meta_notna = meta_drop[meta_drop.qc_location_id.notna()]

    count = meta_notna.groupby(['qc_location_id']).agg(
        {'qc_location_id': ['count']})
    count.columns = count.columns.droplevel(0)

    meta_merge_count = pd.merge(meta_notna.set_index('qc_location_id'), count, left_index=True,
                                right_index=True, how='left').drop_duplicates(subset=['virus_id'])
    meta_merge_country = meta_merge_count.reset_index() \
        .assign(
            rank=lambda x: pd.to_datetime(x.qc_date).groupby(
                x.qc_location_id).rank(ascending=1, method='dense').astype('Int64'),
            count=lambda x: x['count'].astype('Int64'),
            percentile=lambda x: x['rank'] / x['count'],
            xref_id=lambda x: x.xref_id.apply(lambda x: x.split('.')[0])
    )

    genome_merge = pd.concat([meta_merge_country, meta_na], ignore_index=False). \
        dropna(subset=['host'])
    genome_merge_host = genome_merge[genome_merge.host.isin(['Human', 'Homo sapiens',
                                                             'human', 'HUMAN', 'Hman'])]

    genome_rename_columns = {'xref_id': 'genome_id', 'xref_name': 'virus_name',
                             'qc_date': 'qc_sample_collection_date', 'date': 'sample_collection_date'}
    genome_drop_columns = ['qc_location_id', 'release_date']
    genome_columns = ['genome_id', 'gender', 'qc_gender', 'age', 'qc_age', 'sample_collection_date', 'qc_sample_collection_date',
                      'qc_city', 'qc_province', 'country', 'qc_country', 'location', 'qc_location', 'qc_continent', 'count', 'rank', 'percentile', 'data_source',
                      'virus_id', 'virus_name', 'host', 'specimen_source', 'author',
                      'submitter_lab_address', 'address', 'origin_lab', 'origin_lab_address']

    genome_merge_host.rename(columns=genome_rename_columns, inplace=True)
    genome_drop = genome_merge_host.drop(genome_drop_columns, axis=1)

    genome_adjust = genome_drop[genome_columns]

    genome_adjust.to_csv(
        ARTICLE_GENOME,
        sep='\t',
        encoding='utf-8',
        index=False
    )
    print(genome_adjust.shape)
    print(genome_adjust.columns)


if __name__ == '__main__':
    build()
