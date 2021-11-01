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
VARIANT = filepath(rootpath.detect(),
                   'data/sync/KGCOV/raw/downloaded', 'variant_20201109.csv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
NODE_VARIANT = filepath(OUTPUT_DIR,'NODE_Variant.tsv')
def build():
    mutation = pd.read_csv(
        VARIANT,
        encoding='GB2312',
        engine="python",
        error_bad_lines=False
    )

    columns = {
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
        'accession_id': 'xref_id',
        '蛋白': 'protein'
    }

    drop_columns = ['virus_name', 'host', 'country', 'collection_date', 'virus_id']

    mutation = mutation.rename(columns=columns) \
        .drop(drop_columns, axis=1) \
        .drop_duplicates(subset=['variant_id']) \
        .assign(
        variant_id=lambda x: x.variant_id.apply(lambda x: x[12:]),
        var_aa=lambda x: x.var_aa.apply(get_var),
        var_aa_id = lambda x: x.apply(lambda x: change_var(x.synonymous,x.var_aa,x.var_aa_pos),axis=1),
        region = lambda x: x.region.str.replace('exonic','coding')
        )

    mutation.to_csv(
        NODE_VARIANT,
        sep='\t',
        encoding='utf-8',
        index=False
        )
    print(mutation.shape)
    print('success')


if __name__ == '__main__':
    build()