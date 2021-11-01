import rootpath
rootpath.append()
from src.qc.var_pos_match import position
from src.utils.data_file import filepath
import pandas as pd 
import numpy as np 
from src.qc.feature_merge_qc import concat_func


# INPUT
SARA2 = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','sars2.xlsx')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data')
NODE_Feature = filepath(OUTPUT_DIR,'NODE_Feature.tsv')

columns = {
    'primaryAccession':'primary_accession',
    'Name':'name',
    'Gene':'gene',
    'Type':'type',
    'Description':'description',
    'Position':'position',
    'evidenceCode':'evidence_code'
}

use_columns = ['primaryAccession','Type','Description','Position']


def build():
    sars2 = pd .read_excel(
        SARA2,
        usecols=use_columns
    ) \
    .assign(
        feature_id = lambda x: x.primaryAccession + ':' +x.Position
    ) \
    .rename(columns=columns) \
    .drop_duplicates()

    feature_group = sars2.groupby(sars2['feature_id']).apply(concat_func).reset_index()
    feature_group_merge = feature_group.merge(sars2[['feature_id','primary_accession','position']],how='left',on='feature_id'). \
        drop_duplicates(subset='feature_id')

    feature_group_merge.to_csv(
        NODE_Feature,
        sep='\t',
        encoding='utf-8',
        index=False
    )
    
    print(feature_group_merge.shape)
    print('sucess')

if __name__ == '__main__':
    build()