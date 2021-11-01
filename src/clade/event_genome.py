import rootpath
rootpath.append()
from src.utils.data_file import filepath
import pandas as pd
from src.qc.mkdir import mkdir


# INPUT
INPUT_FILE = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/manual', 'EventGenome.xlsx')

#OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
NODE_TSV = filepath(OUTPUT_DIR, 'NODE_Event.tsv')
LINK_TSV = filepath(OUTPUT_DIR, 'LINK_RELATED_EventGenome.tsv')

def build():
    pd.read_excel(INPUT_FILE, sheet_name='Node') \
        .drop_duplicates() \
        .to_csv(
            NODE_TSV, 
            sep='\t', 
            index=False, 
            encoding='utf-8'
        )

    pd.read_excel(INPUT_FILE, sheet_name='Link') \
        .drop_duplicates() \
        .to_csv(
            LINK_TSV, 
            sep='\t', 
            index=False, 
            encoding='utf-8'
        )
    print("sucess")

if __name__ == '__main__':
    build()