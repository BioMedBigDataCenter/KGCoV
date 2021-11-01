# 解析genometxt to dataframe
import rootpath 
rootpath.append()
import pandas as pd
import numpy as np
from datetime import datetime
import time
import math
import re
from tqdm import tqdm
from time import sleep
from src.qc.genome_age_qc import get_age
from src.qc.genome_date_qc import get_datetime
from src.qc.gender_qc import get_gender
import math
import re
from src.utils.data_file import filepath
from src.qc.location_qc import get_country
from src.qc.mkdir import mkdir

# INPUT
VIGTK_TXT = filepath(rootpath.detect(),'data/sync/KGCOV/raw/downloaded','genome_metadata_20211020.txt')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/meta')
mkdir(OUTPUT_DIR)
Genome_Table = filepath(OUTPUT_DIR,'genome_table.tsv')

def build():
    genome_new = open(VIGTK_TXT,'r',encoding='utf-8')
    titles= genome_new.readline().split('\t')

    lines = genome_new.readlines()[1:]

    genome_result = {}
    for title in titles:
        genome_result[title.strip()] = []
    print(genome_result)

    for line in lines:
        data = line.split('\t')
        for count in range(len(titles)):
            genome_result[titles[count].strip()].append(data[count])
    genome_df = pd.DataFrame(genome_result)
    genome_date = genome_df.assign(
        qc_collection_date=lambda x: x.collection_date.apply(get_datetime)
    ) \
        .dropna(subset=['qc_collection_date']) \
        .set_index('qc_collection_date')['2019-11-01':'2020-11-30'].reset_index() 
    genome_date.to_csv(
                Genome_Table,
                sep='\t',
                index=False,
                encoding='utf-8'
            )

    print(genome_date.shape)
if __name__ == '__main__':
    build()