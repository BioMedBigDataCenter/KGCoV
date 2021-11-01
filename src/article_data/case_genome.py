import rootpath
rootpath.append()
from src.utils.data_file import filepath
from time import sleep
import numpy as np
import pandas as pd
from src.qc.mkdir import mkdir

# INPUT
INPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output')
CASE_GENOME = filepath(INPUT_DIR,'graph_data','LINK_MATCH_CaseGenome.tsv')
ARTICLE_CASE = filepath(INPUT_DIR,'target','case.tsv')
ARTICLE_GENOME = filepath(INPUT_DIR,'target','genome.tsv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/target')
mkdir(OUTPUT_DIR)
ARTICLE_CASE_GENOME = filepath(OUTPUT_DIR, 'case_genome.tsv')



# link_case_genome

genome_usecols =  ['genome_id','qc_gender','qc_age','qc_sample_collection_date', \
        'qc_country','qc_location','data_source','virus_id','qc_city','qc_province']
case_usecols = ['case_id','qc_gender','qc_age','qc_case_reported_date','qc_country','qc_location', \
               'data_source','qc_city','qc_province']
rename_columns = {
    'genome_genome_id':'genome_id',
    'genome_qc_sample_collection_date':'genome_qc_date',
    'case_case_id':'case_id',
    'case_qc_case_reported_date':'case_qc_date'
}
columns = ['genome_id','case_id','genome_qc_gender','genome_qc_age','genome_qc_date', 'genome_qc_country', \
        'genome_qc_location','genome_data_source','case_qc_gender','case_qc_age','case_qc_date', 'case_qc_country',\
            'case_qc_location', 'case_data_source','curated']  
            

def build():
    case_genome = pd.read_csv(
        CASE_GENOME,
        sep='\t',
        header = 0,
        names=[
            'virus_id',
            'case_id',
            'curated'
        ]
    )
    print(case_genome.columns)
    print(case_genome.head())

    case = pd.read_csv(
        ARTICLE_CASE,
        sep='\t',
        usecols=case_usecols
    )
    print(case.add_prefix('case_').columns)   

    genome = pd.read_csv(
        ARTICLE_GENOME,
        sep='\t',
        usecols=genome_usecols
    )
    print(genome.add_prefix('genome_').columns) 

    case_genome_info = case_genome. \
        merge(genome.add_prefix('genome_'),left_on='virus_id',right_on='genome_virus_id',how='left'). \
            drop(['virus_id','genome_virus_id'],axis=1). \
        merge(case.add_prefix('case_'),left_on='case_id',right_on='case_case_id',how='left'). \
            drop(['case_case_id'],axis=1). \
            rename(columns=rename_columns)[columns]. \
                sort_values(by=['curated'],ascending=False)

    case_genome_info.to_csv(
        ARTICLE_CASE_GENOME,
        sep='\t',
        encoding='utf-8',
        index=False
    )
    print(case_genome_info.shape)
    print(case_genome_info.columns)
    print(case_genome_info.head())
    print(case_genome_info[(case_genome_info.genome_qc_location==case_genome_info.case_qc_location)| \
        (pd.isnull(case_genome_info.genome_qc_location)&pd.isnull(case_genome_info.case_qc_location))])
if __name__ == '__main__':
    build()
