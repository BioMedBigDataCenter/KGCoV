# ## Contient
#
# ### Link
#
# https://www.geonames.org/countries/
#
# ### API
#
# https://download.geonames.org/export/dump/countryInfo.txt

import pandas as pd
import rootpath
rootpath.append()

from src.utils.network import get_csv
from src.utils.data_file import filepath
from src.utils.step import console_log_saved
from src.qc.mkdir import mkdir


# OUTPUT
OUTPUT = filepath(rootpath.detect(),'data/sync/KGCOV/target')
mkdir(OUTPUT)
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/controlled_vocab')
mkdir(OUTPUT_DIR)
CONTINENT_MAP = filepath(OUTPUT_DIR, 'continent_map.tsv')

CONTINENT_FULL = {
    'AF': 'Africa',
    'AN': 'Antarctica',
    'AS': 'Asia',
    'EU': 'Europe',
    'NA': 'North america',
    'OC': 'Oceania',
    'SA': 'South america',
}


def build():
    df, comments = get_csv(
        'https://download.geonames.org/export/dump/countryInfo.txt',
        sep='\t', header=None,
        comment_prefix='#'
    )
    df.columns = comments.split(
        b'\n')[-1].decode('utf-8').lstrip('# ').split('\t')
    iso_series = df.set_index('Continent').ISO  # type: ignore

    pd.Series(CONTINENT_FULL, name='Continent').to_frame() \
        .join(iso_series) \
        .dropna() \
        .to_csv(CONTINENT_MAP, sep='\t', index=False)
    console_log_saved(CONTINENT_MAP)
    print("sucess")


if __name__ == "__main__":
    build()
