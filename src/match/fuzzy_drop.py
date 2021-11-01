import pandas as pd
import numpy as np
import rootpath
rootpath.append()
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir

# INPUT
EXACT_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','exact_matchall_text_meta.csv')
FUZZY_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','text_meta_matchall.csv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match')
mkdir(OUTPUT_DIR)
FUZZY_DROP = filepath(OUTPUT_DIR,'fuzzy_drop.csv')



def build():
    # 读精确匹配的数据
    exact = pd.read_csv(
        EXACT_TABLE
    )
    exact_virus = exact['virus_id'].drop_duplicates().values.tolist()

    # 读所有匹配到的数据
    fuzzy = pd.read_csv(
        FUZZY_TABLE
    )
    fuzzy_drop = fuzzy[
        ~fuzzy.virus_id.isin(exact_virus)
    ]

    fuzzy_drop.to_csv(
        FUZZY_DROP,
        index=False,
        encoding='utf-8'
    )
    print('sucess')

if __name__ == '__main__':
    build()
