import pandas as pd
import numpy as np
import rootpath
rootpath.append()
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir


# INPUT
EXACT_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','exact_matchall_text_meta.csv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match')
mkdir(OUTPUT_DIR)
EXACT_DROP = filepath(OUTPUT_DIR,'exact_drop_noage_gender.csv')


def build():
    exact = pd.read_csv(EXACT_TABLE)

    exact_drop = exact[~(
        (exact['age_meta'] == np.nan) &
        (exact['age_wiki'] == np.nan) &
        (exact['gender_meta'] == np.nan) &
        (exact['gender_wiki'] == np.nan)
    )] \
        .to_csv(
            EXACT_DROP,
            index=False,
            encoding='utf-8'
        )
    print('sucess')
    # print(exact_drop.shape)

if __name__ == '__main__':
    build()

