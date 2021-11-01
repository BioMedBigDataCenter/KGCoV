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
MATCH = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data', 'LINK_MATCH_CaseGenome.tsv')
# EXACT_MATCH = filepath(rootpath.detect(),'data/sync/output/match','exact_matchall_text_meta.csv')
CASE_TSV = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data', 'NODE_Case_new.tsv')
META = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data','NODE_Genome.tsv')
# TOP5 = filepath(rootpath.detect(),'data/sync/raw/manual','top5-二次审编完成.csv')
# EVENT = filepath(rootpath.detect(), 'data/sync/raw/manual', 'EventGenome.xlsx')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
DATA_Country = filepath(OUTPUT_DIR,'DATA_Country(CV).tsv')

def build():
    case = pd.read_csv(
        CASE_TSV,
        sep='\t'
    )

    meta = pd.read_csv(
        META,
        sep='\t'
    )
    match = pd.read_csv(
        MATCH,
        sep='\t',
        encoding='utf-8'
    )
    case_all = list(set(match.new_case_id.values.tolist()))
    virus_all = list(set(match.virus_id.values.tolist()))
    case_country = case[['qc_country','new_case_id','qc_location_id']]
    virus_country = meta[['qc_country','virus_id','qc_location_id']]
    case_name = case_country[case_country.new_case_id.isin(case_all)]
    virus_name = virus_country[virus_country.virus_id.isin(virus_all)]
    country = case_name.append(virus_name,ignore_index=True)
   
    country_choose = country[['qc_location_id','qc_country']]
    country_dupli = country_choose.drop_duplicates()
    country_dupli = country_dupli.dropna()
    country_dupli.to_csv(
            DATA_Country,
            sep='\t',
            index=False,
            encoding='utf-8'
        )
    print(country_dupli.shape)
    print(country_dupli.qc_location_id.nunique())
    print('sucess')
if __name__ == '__main__':
    build()

