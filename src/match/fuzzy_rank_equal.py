import pandas as pd
import numpy as np
import rootpath
rootpath.append()
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir


# INPUT
FUZZY_DROP = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','fuzzy_drop.csv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match')
mkdir(OUTPUT_DIR)
RANK_EQUAL = filepath(OUTPUT_DIR,'fuzzy_rank_equal.csv')


def build():
    fuzzy_drop = pd.read_csv(
        FUZZY_DROP
    )

    rank_match = fuzzy_drop[
        fuzzy_drop.rank_meta == fuzzy_drop.rank_wiki
    ]

    rank_match.to_csv(
        RANK_EQUAL,
        index=False,
        encoding='utf-8'
    )
    print('sucess')

if __name__ == '__main__':
    build()
