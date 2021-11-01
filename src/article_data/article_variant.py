# variant

import rootpath
rootpath.append()
from src.qc.var_split import get_var
from src.utils.data_file import filepath
from src.qc.variant_var_aa_id import change_var
from src.qc.mkdir import mkdir
import pandas as pd
import numpy as np
from tqdm.notebook import tqdm
import os
from datetime import datetime
import time


# Variation
# INPUT
VARIANT = filepath(rootpath.detect(),'data/sync/KGCOV/raw/downloaded', 'variant_20201109.csv')
ARTICLE_GENOME = filepath(rootpath.detect(), 'data/sync/KGCOV/output/target','genome.tsv')
FEATURE = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data','NODE_Feature.tsv')
FEATURE_VARIANT = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data','LINK_IMPACT_VariantFeature.tsv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/target')
mkdir(OUTPUT_DIR)
NODE_VARIANT = filepath(OUTPUT_DIR,'variant.tsv')

variant_columns = ['variant_id','genome_id','virus_id','taxon_id','position','ref','alt','type','region','gene', \
                  'var_aa','var_aa_pos','synonymous','mutation_frequency','protein','var_aa_id']

def build():
    mutation = pd.read_csv(
        VARIANT,
        encoding='GB2312',
        engine="python",
        error_bad_lines=False
    )

    genome = pd.read_csv(ARTICLE_GENOME,sep='\t',encoding='utf-8',usecols=['virus_id','genome_id'])

    columns = {
        'accession_id': 'genome_id',
        '变异': 'variant_id',
        '物种编号': 'taxon_id',
        '染色体': 'chrom',
        '位置': 'position',
        'ref': 'ref',
        'alt': 'alt',
        '类型': 'type',
        '区域': 'region',
        '基因': 'gene',
        '氨基酸变化': 'var_aa',
        '氨基酸变化位置': 'var_aa_pos',
        '同义/非同义': 'synonymous',
        '宿主': 'hosts',
        '国家': 'countries',
        '突变频率': 'mutation_frequency',
        '基因组编号': 'virus_id',
        '毒株名称': 'virus_name',
        '宿主.1': 'host',
        '采样国家': 'country',
        '采样时间': 'collection_date',
        '蛋白': 'protein'
    }

    drop_columns = ['virus_name', 'host', 'country', 'collection_date','chrom']

    mutation = mutation.rename(columns=columns) \
        .drop(drop_columns, axis=1) \
        .drop_duplicates() \
        .assign(
        variant_id=lambda x: x.variant_id.apply(lambda x: x[12:]),
        var_aa=lambda x: x.var_aa.apply(get_var),
        var_aa_id = lambda x: x.apply(lambda x: change_var(x.synonymous,x.var_aa,x.var_aa_pos),axis=1)
        )


    mutation_merge = mutation.merge(genome,on='virus_id',how='left')
    mutation_drop = mutation_merge.dropna(subset=['genome_id'])
    variant_adjust = mutation_drop[variant_columns]

    feature = pd.read_csv(
        FEATURE,
        sep='\t',
        encoding='utf-8', 
        usecols=['feature_id','type','description']
        )
    feature_rename = feature.rename(columns={'type':'domain_type'})
    feature_variant = pd.read_csv(
        FEATURE_VARIANT,
        sep='\t',
        encoding='utf-8'
        )
    feature_variant_match = feature_variant.merge(feature_rename,on='feature_id',how='left')
    feature_variant_merge = pd.merge(variant_adjust,feature_variant_match,on='variant_id',how='left').drop_duplicates()
    feature_variant_merge.rename(columns={'feature_id':'domain_id'},inplace=True)

    feature_variant_merge.to_csv(
        NODE_VARIANT,
        sep='\t',
        encoding='utf-8',
        index=False
        )
    print(mutation.shape)
    print(mutation_merge.shape)
    print(feature_variant_merge.columns)
    print(feature_variant_merge.shape)
    print('success')
if __name__ == '__main__':
    build()

