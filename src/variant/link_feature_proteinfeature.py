import rootpath
rootpath.append()
from src.utils.data_file import filepath
import pandas as pd 
import numpy as np 

# INPUT
PROTEIN = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data','NODE_Protein(CV).tsv')
FEATURE = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data','NODE_Feature.tsv')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data')
LINK_FEATURE_ProteinFeature = filepath(OUTPUT_DIR,'LINK_FEATURE_ProteinFeature.tsv')


def build():
    protein = pd.read_csv(
        PROTEIN,
        sep='\t',
        encoding='utf-8'
        )

    feature = pd.read_csv(
        FEATURE,
        sep='\t',
        encoding='utf-8'
        )

    protein_feature = feature[['feature_id','primary_accession']] \
        .merge(protein[['entry']],
            how='left',
            left_on='primary_accession',
            right_on='entry'
            )

    protein_feature_drop = protein_feature.drop(['primary_accession'],axis=1)

    protein_feature_drop.to_csv(
        LINK_FEATURE_ProteinFeature,
        sep='\t',
        encoding='utf-8',
        index=False
        )

    print('sucess')
    print(protein_feature_drop.shape)

if __name__ == "__main__":
    build()
