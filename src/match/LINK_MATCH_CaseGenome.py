import rootpath
rootpath.append()
from src.utils.data_file import filepath
import pandas as pd
import numpy as np
import re
from src.qc.mkdir import mkdir



# INPUT
EXACT = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','exact_drop_noage_gender.csv')
CASEID_DICT = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','caseid_dict.tsv')
GENOME = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data','NODE_Genome.tsv')
CURATED_MATCH = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','curated_case_genome.tsv')







# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
LINK_MATCH_CaseGenome = filepath(OUTPUT_DIR,'LINK_MATCH_CaseGenome.tsv')

def build():
    columns = {
        'case_id':'new_case_id'
    }

    caseid = pd.read_csv(
    CASEID_DICT,
    sep='\t',
    encoding='utf-8'
    )

    caseid_dict = {
        **caseid.set_index('case_id')['new_case_id'].to_dict()
    }

    genome = pd.read_csv(
        GENOME,
        sep='\t',
        encoding = 'utf-8'
    )


    curated_match = pd.read_csv(
        CURATED_MATCH,
        sep='\t'
    )

    # 选取exact中除了top10的部分
    exact = pd.read_csv(
        EXACT,
        encoding='utf-8',
        usecols=['virus_id','new_case_id']
    ) \
        .rename(columns=columns) \
        .merge(
            genome[['virus_id','rank']]
        )
    
    exact_drop_rank_15 = exact[exact['rank']>15][['virus_id','new_case_id']]
  

    virus_except = list(set(curated_match.virus_id.tolist()))
    


    exact_drop = exact_drop_rank_15[~exact_drop_rank_15.virus_id.isin(virus_except)] \
        .assign(
            curated = False
        )

    merge_list = [curated_match,exact_drop]
    case_genome = pd.concat(merge_list,ignore_index=True) \
        .drop_duplicates() \
        .dropna(subset=['new_case_id'])\
        .drop_duplicates(subset=['virus_id','new_case_id'],keep='first')

    

    genome = pd.read_csv(
        GENOME,
        sep='\t',
        encoding='utf-8'
    )

    match_virus = case_genome.merge(genome[['qc_age','qc_gender','virus_id']],how='left')

    match_drop = match_virus.dropna(subset=['qc_age','qc_gender'],how='all')[['virus_id','new_case_id','curated']]
    
    match_drop.to_csv(
            LINK_MATCH_CaseGenome,
            sep='\t',
            encoding='utf-8',
            index=False
        )
    print('success')


if __name__ == '__main__':
    build()



