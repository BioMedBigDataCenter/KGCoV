import pandas as pd
import numpy as np
import rootpath
rootpath.append()
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir


# INPUT
EXACT_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','exact_matchall_text_meta.csv')
FUZZY_TABLE = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','text_meta_matchall.csv')
EXACT_DROP = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','exact_drop_noage_gender.csv')
RANK_EQUAL = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','fuzzy_rank_equal.csv')
SIMILAR_AGE = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match','fuzzy_similar_age.csv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text/match')
mkdir(OUTPUT_DIR)
TOP5_MERGED = filepath(OUTPUT_DIR,'virus_top5_merge.csv')


def build():
    # 读所有匹配到的数据
    fuzzy = pd.read_csv(
        FUZZY_TABLE
    )
    # 读精确匹配的数据
    exact = pd.read_csv(
        EXACT_TABLE
    )

    # 所有匹配到的top5的数据
    fuzzy_top5 = fuzzy[fuzzy['rank_meta'].astype('int') < 6]

    # 精确匹配删除没有年龄性别剩下数据中的top5
    exact_drop = pd.read_csv(
        EXACT_DROP
    )
    exact_drop_top5 = exact_drop[exact['rank_meta'].astype('int') < 6]
    exact_drop_top5['rules'] = 'exact_match'

    # rank相等数据中的top5
    rank = pd.read_csv(
        RANK_EQUAL
    )
    rank_top5 = rank[rank['rank_meta'].astype('int') < 6]
    rank_top5['rules'] = 'rank_equal'

    # similar age中的top5
    similar_age = pd.read_csv(
        SIMILAR_AGE
    )
    similar_age_top5 = similar_age[similar_age['rank_meta'].astype('int') < 6]
    similar_age_top5['rules'] = 'similar_age'

    # 精确匹配、rank_equal、similar_age的virus_id的和
    exact_virus = exact['virus_id'].drop_duplicates().values.tolist()
    # exact_drop_top5_virus = exact_drop_top5['virus_id'].drop_duplicates().values.tolist()
    rank_top5_virus = rank_top5['virus_id'].drop_duplicates().values.tolist()
    similar_age_top5_virus = similar_age_top5['virus_id'].drop_duplicates(
    ).values.tolist()
    virus = exact_virus + rank_top5_virus + similar_age_top5_virus

    # 除了上面加和的virus_id，其他的标为other_top5
    other_top5 = fuzzy_top5[~fuzzy_top5.virus_id.isin(virus)]
    other_top5['rules'] = 'other_top5'
    top5 = pd.concat([
        exact_drop_top5, rank_top5,
        similar_age_top5, other_top5
        ],
        ignore_index=True) \
        .to_csv(
            TOP5_MERGED,
            encoding='utf-8',
            index=False
        )

if __name__ == '__main__':
    build()


# exact_drop_top5.to_csv('./data/output/total_data_save/exact_drop_top5_0617.csv',encoding='utf-8',index=False)
# other_top5.to_csv('./data/output/total_data_save/other_top5_0617.csv',encoding='utf-8',index=False)
