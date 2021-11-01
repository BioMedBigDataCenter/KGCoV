# 创造sample
import rootpath
rootpath.append()
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir
import pandas as pd 
import numpy as np


FILE = ['NODE_Case_new','NODE_Genome','NODE_Variant','NODE_Protein(CV)','NODE_Symptom(CV)','NODE_Location(CV)',\
         'NODE_Event','DATA_Country(CV)','LINK_MATCH_CaseGenome','LINK_VARIANT_GenomeVariant','LINK_AFFECT_VariantProtein',\
         'LINK_PRESENT_CaseClinicalInfo','LINK_CONTACT_CaseContactInfo','LINK_TRAVEL_CaseTravelInfo',\
         'LINK_RELATED_EventGenome','NODE_Feature','LINK_IMPACT_VariantFeature','LINK_FEATURE_ProteinFeature']

def build():
    for i in FILE:
        #INPUT
        INPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data',i)
        OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data/sample')
        mkdir(OUTPUT_DIR)
        OUTPUT_FILE = filepath(OUTPUT_DIR,i)
        sample = pd.read_csv(INPUT_DIR + '.tsv',sep='\t',encoding='utf-8') \
            .head()
        sample.to_csv(
                OUTPUT_FILE + '.tsv',
                sep='\t',
                encoding='utf-8',
                index=False
            )
    print('sucess')

if __name__ == '__main__':
    build()