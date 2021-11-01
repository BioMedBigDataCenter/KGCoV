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
SIMILAR_AGE = filepath(OUTPUT_DIR,'fuzzy_similar_age.csv')


def build():
    fuzzy_drop = pd.read_csv(
        FUZZY_DROP
    ) \
        .assign(
            age_meta=lambda x: x.age_meta.fillna(value=0).astype('int'),
            age_wiki=lambda x: x.age_wiki.fillna(value=0).astype('int'),
            date_meta=lambda x: pd.to_datetime(x.date_meta),
            date_wiki=lambda x: pd.to_datetime(x.date_wiki),
            abs_delta_coll_days=lambda x: abs((x.date_meta-x.date_wiki).dt.days),
            decade_age_meta=lambda x: np.ceil(x.age_meta/10),
            decade_age_wiki=lambda x: np.ceil(x.age_wiki/10)
        )

    # fuzzy_drop.date_meta = pd.to_datetime(fuzzy_drop.date_meta)
    # fuzzy_drop.date_wiki = pd.to_datetime(fuzzy_drop.date_wiki)
    # fuzzy_drop.age_meta = fuzzy_drop.age_meta.astype('int')
    # fuzzy_drop.age_wiki = fuzzy_drop.age_wiki.astype('int')
    # fuzzy_drop.age_meta.value_counts()
    # fuzzy_drop = fuzzy_drop.assign(
    #     abs_delta_coll_days=lambda x: abs((x.date_meta-x.date_wiki).dt.days),
    #     decade_age_meta=lambda x: np.ceil(x.age_meta/10),
    #     decade_age_wiki=lambda x: np.ceil(x.age_wiki/10)
    # )
    ### 怎么合并pd.to_datetime()

    # fuzzy_exact_match date<3,gender=gender,age十位数相等
    # fuzzy_exact_match date<3,gender=gender,age=0
    fuzzy_drop_exact = fuzzy_drop[
        ((fuzzy_drop.gender_meta == fuzzy_drop.gender_wiki) &
        (fuzzy_drop.abs_delta_coll_days < 3) &
            (fuzzy_drop.decade_age_meta == fuzzy_drop.decade_age_wiki)) |
        ((fuzzy_drop.gender_meta == fuzzy_drop.gender_wiki) &
        (fuzzy_drop.abs_delta_coll_days < 3) &
        (fuzzy_drop.age_meta == np.nan) |
        (fuzzy_drop.age_wiki == np.nan))
    ] \
        .drop([
            'abs_delta_coll_days', 'decade_age_meta', 'decade_age_wiki'
        ],axis=1) \
        .to_csv(
            SIMILAR_AGE,
        index=False,
        encoding='utf-8'
        )
    print('sucess')
if __name__ == '__main__':
    build()
