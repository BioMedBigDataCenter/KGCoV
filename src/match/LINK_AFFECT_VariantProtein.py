import rootpath
rootpath.append()
from src.utils.data_file import filepath
import pandas as pd
import numpy as np
from tqdm.notebook import tqdm
import os
from datetime import datetime
import time
from src.qc.mkdir import mkdir

# INPUT
VARIANT = filepath(rootpath.detect(),
                   'data/sync/KGCOV/raw/downloaded', 'variant_20201109.csv')
PROTEIN = filepath(rootpath.detect(),
                   'data/sync/KGCOV/output/graph_data', 'NODE_Protein(CV).tsv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
LINK_AFFECT_VariantProtein = filepath(OUTPUT_DIR,'LINK_AFFECT_VariantProtein.tsv')

def build():
    variation = pd.read_csv(
        VARIANT,
        encoding='GB2312',
        engine='python',
        error_bad_lines=False,
        usecols=['蛋白', '变异']
    )

    protein = pd.read_csv(
        PROTEIN,
        sep='\t',
        usecols=['entry', 'vigtk_alias']
    )

    columns_variant = {
        '蛋白': 'protein',
        '变异': 'variant_id'
    }

    columns_protein = {
        'entry': 'uniprot'
    }

    drop_columns = ['protein_split', 'vigtk_alias', 'protein']


    genome_variant_drop = variation.drop_duplicates() \
        .dropna(subset=['蛋白']) \
        .rename(columns=columns_variant) \
        .assign(
        variant_id=lambda x: x.variant_id.apply(lambda x: x[12:])
        )

    var_pro_split = genome_variant_drop['protein'].str.split(',', expand=True) \
        .stack() \
        .reset_index(level=1, drop=True) \
        .rename('protein_split')

    variant_protein = genome_variant_drop.join(var_pro_split) \
        .merge(protein, left_on='protein_split', right_on='vigtk_alias', how='left') \
        .drop(drop_columns, axis=1) \
        .rename(columns=columns_protein) \
        .to_csv(
        LINK_AFFECT_VariantProtein,
        index=False,
        sep='\t',
        encoding='utf-8'
        )
    print('sucess')
    
if __name__ == '__main__':
    build()

