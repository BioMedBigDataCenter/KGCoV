import rootpath
rootpath.append()
from src.utils.data_file import filepath
from time import sleep
import numpy as np
import pandas as pd
import re
from src.qc.mkdir import mkdir

# INPUT
INPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data')
NODE_GENOME = filepath(INPUT_DIR,'NODE_Genome.tsv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/target')
mkdir(OUTPUT_DIR)
ARTICLE_GENOME = filepath(OUTPUT_DIR, 'genome.tsv')

# genome

genome_rename_columns = {'xref_id':'genome_id','xref_name':'virus_name','qc_date':'qc_sample_collection_date','date':'sample_collection_date'}
genome_drop_columns = ['qc_location_id','release_date']
genome_columns = ['genome_id','gender','qc_gender','age','qc_age','sample_collection_date','qc_sample_collection_date', \
        'qc_city','qc_province','country','qc_country','location','qc_location','qc_continent','count','rank','percentile', 'data_source',\
                 'virus_id','virus_name','host','specimen_source','author', \
                  'submitter_lab_address','address','origin_lab','origin_lab_address']

def build():
    genome = pd.read_csv(
        NODE_GENOME,
        sep='\t',
        encoding='utf-8'
    )
    genome.rename(columns=genome_rename_columns,inplace=True)
    genome_drop = genome.drop(genome_drop_columns,axis=1)
    print(genome_drop.columns)

    genome_adjust = genome_drop[genome_columns]

    genome_adjust.to_csv(
        ARTICLE_GENOME,
        sep='\t',
        encoding='utf-8',
        index=False
    )
    print(genome_adjust.shape)

if __name__ == '__main__':
    build()