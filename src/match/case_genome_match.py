import rootpath
rootpath.append()
from src.utils.data_file import filepath

from src.utils.step import Step
from src.match.meta_text_match import build as fuzzy_match_all
from src.match.exact_match import build as fuzzy_exact_match
from src.match.exact_drop_noage_nogender import build as exact_drop
from src.match.fuzzy_drop import build as fuzzy_drop_exact
from src.match.fuzzy_rank_equal import build as fuzzy_rank_equal
from src.match.fuzzy_similar_age import build as fuzzy_similar_age
from src.match.match_meta_top5 import build as meta_top5


def build():
    step = Step('start case and genome match')

    step('fuzzy match all case and genome')
    fuzzy_match_all()

    step('fuzzy exact match')
    fuzzy_exact_match()
    exact_drop()

    step('create meta_top5 file')
    fuzzy_drop_exact()
    fuzzy_rank_equal()
    fuzzy_similar_age()
    meta_top5()
    print('sucess')

if __name__ == '__main__':
    build()

