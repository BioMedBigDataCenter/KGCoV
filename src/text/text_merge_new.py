import rootpath
rootpath.append()
import pandas as pd
import numpy as np 
from src.utils.data_file import filepath
from src.utils.step import Step
from src.qc.mkdir import mkdir

# INPUT
KAGGLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/text','kaggle_new_caseid.tsv')
DXY = filepath(rootpath.detect(),'data/sync/KGCOV/output/text','dxy_new_caseid.tsv')
WIKI = filepath(rootpath.detect(),'data/sync/KGCOV/output/text','wiki_merge_new_caseid.tsv')
MANUAL = filepath(rootpath.detect(),'data/sync/KGCOV/output/text','manual_new_caseid.tsv')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text')
mkdir(OUTPUT_DIR)
MERGED_TABLE = filepath(OUTPUT_DIR,'text_new_merge_all.tsv')

def build():
    step = Step('TextMerge')

    kaggle = pd.read_csv(
        KAGGLE,
        sep='\t',
        encoding='utf-8'
    )



    wiki = pd.read_csv(
        WIKI,
        sep='\t',
        encoding='utf-8'
    )

    manual = pd.read_csv(
        MANUAL,
        sep='\t',
        encoding='utf-8'
    )

    merge_list = [kaggle,wiki,manual]

    df_merge = pd.concat(merge_list,ignore_index=True)
    df_merge.to_csv(
            MERGED_TABLE,
            sep='\t',
            encoding='utf-8',
            index=False
        )

    print(df_merge.shape)
    print('sucess')

if __name__ == '__main__':
    build()