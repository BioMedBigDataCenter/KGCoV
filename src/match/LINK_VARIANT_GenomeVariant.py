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

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
LINK_VARIANT_GenomeVariant = filepath(OUTPUT_DIR,'LINK_VARIANT_GenomeVariant.tsv')


def build():
    columns = {
        '变异': 'variant_id',
        '基因组编号': 'virus_id'
    }


    variation = pd.read_csv(
        VARIANT,
        encoding='GB2312',
        engine="python",
        error_bad_lines=False,
        usecols=['变异', '基因组编号']
        ) \
        .drop_duplicates() \
        .rename(columns=columns) \
        .assign(
        variant_id=lambda x: x.variant_id.apply(lambda x: x[12:])
        ) \
        .to_csv(
        LINK_VARIANT_GenomeVariant,
        index=False,
        sep='\t',
        encoding='utf-8'
        )
    print('success')

if __name__ == '__main__':
    build()

