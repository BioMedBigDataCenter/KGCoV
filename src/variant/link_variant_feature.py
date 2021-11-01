import rootpath
rootpath.append()
from src.qc.var_pos_match import position
from src.utils.data_file import filepath
import pandas as pd 
import numpy as np 

# INPUT
SARA2 = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','sars2.xlsx')
PROTEIN = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data','NODE_Protein(CV).tsv')
VARIANT = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data','NODE_Variant.tsv')
VARIANT_PROTEIN = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data','LINK_AFFECT_VariantProtein.tsv')

# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data')
LINK_FUNCTION_VariantFeature = filepath(OUTPUT_DIR,'LINK_IMPACT_VariantFeature.tsv')



def build():
    sars2 = pd .read_excel(
        SARA2
    ) \
        .drop_duplicates()

    protein = pd.read_csv(
        PROTEIN,
        sep='\t',
        encoding='utf-8',
        usecols=['entry','protein_names','vigtk_alias']
    )

    variant_protein = pd.read_csv(
        VARIANT_PROTEIN,
        sep='\t',
        encoding='utf-8'
    )

    variant = pd.read_csv(
        VARIANT,
        sep='\t',
        encoding='utf-8'
    )

    merge = pd.merge(sars2,protein,left_on='primaryAccession',right_on='entry',how='left') \
        .assign(
            feature_id = lambda x: x.primaryAccession + ':' + x.Position
        ) \
        .dropna(subset=['vigtk_alias']) \
        .merge(variant_protein,left_on='entry',right_on='uniprot',how='left') \
        .merge(variant,on='variant_id',how='left')

    merge['t'] = merge['var_aa_pos'].str.isdigit()
    merge_pos = merge[merge['t'] == True]
    merge_pos_need = merge_pos.assign(
        need = lambda x: x.apply(lambda x: position(x.Position,x.var_aa_pos),axis=1)
    )
    
    merge_pos_choose = merge_pos_need[merge_pos_need.need==True]
    merge_choose = merge_pos_choose[['variant_id','feature_id']]
    merge_choose.to_csv(
        LINK_FUNCTION_VariantFeature,
        sep='\t',
        encoding='utf-8',
        index=False
    )
    print(merge_choose.shape)
    print(merge_pos_need.need.value_counts())
if __name__ == '__main__':
    build()

