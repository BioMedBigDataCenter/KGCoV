import rootpath
rootpath.append()
from src.utils.data_file import filepath
import pandas as pd
import numpy as np
import re
from src.qc.mkdir import mkdir



# INPUT
CLINICAL_XLSX = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/manual', '临床症状结构化-0630.xlsx')
TOP5 = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','top5-二次审编完成.csv')
SYMPTOM_MAP = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','symptom_map.xlsx')
CASEID_DICT = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','caseid_dict.tsv')
WIKI_CLINICAL = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','Wiki-临床症状结构化合并.xlsx')
WIKI_MANUAL = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','Wiki-数据清洗合并.xlsx')
ARTICLE_CLINICAL = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','文献中病例临床症状审编-汇总.xlsx')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
CLINICAL = filepath(OUTPUT_DIR,'LINK_PRESENT_CaseClinicalInfo.tsv')
NODE_SYMPTOM = filepath(OUTPUT_DIR,'NODE_Symptom(CV).tsv')

def build():
    columns = {
        'PTD': 'case_id',
        'clinical_text': 'description',
        '时间': 'qc_date',
        '症状': 'symptom_merge'
    }

    wiki_columns = {
         'clinical_text': 'description',
        '时间': 'qc_date',
        '症状': 'symptom_merge'
    }

    article_columns = {
        'PTD': 'case_id',
        'clinical_text': 'description',
        '时间': 'qc_date',
        '症状': 'symptom_merge',
        'patient_text':'text_patient'
    }

    article_drop_columns = ['Source.Name','PMID','patient_id']

    clinical_top5 = pd.read_excel(
        CLINICAL_XLSX
    )

    url_top5 = pd.read_csv(
        TOP5,
        usecols=['PTD','source_wiki'],
        encoding='GB2312'
    ) \
        .assign(
            case_id = lambda x: x.PTD.replace(r'PT.',r'C',regex=True),
            url = lambda x: x.source_wiki
        )[['case_id','url']]

    clinical_url_top5 = clinical_top5.rename(columns=columns) \
        .merge(url_top5, on='case_id', how='left') \
        .drop_duplicates()

    clinical_wiki = pd.read_excel(
    WIKI_CLINICAL
    ) \
    .assign(
        case_id = lambda x: x.PTD.replace(r'PT.',r'C',regex=True),
        )

    url_wiki = pd.read_excel(
        WIKI_MANUAL,
        usecols=['PTD','source_wiki']
    ) \
        .assign(
            case_id = lambda x: x.PTD.replace(r'PT.',r'C',regex=True),
            url = lambda x: x.source_wiki
        )[['case_id','url']]

    clinical_url_wiki = clinical_wiki.rename(columns=wiki_columns) \
        .merge(url_wiki, on='case_id', how='left') \
        .drop_duplicates()

    article_clinical = pd.read_excel(
        ARTICLE_CLINICAL
    )

    symptom_map = pd.read_excel(
        SYMPTOM_MAP
    )

    caseid = pd.read_csv(
    CASEID_DICT,
    sep='\t',
    encoding='utf-8'
    )

    caseid_dict = {
        **caseid.set_index('case_id')['new_case_id'].to_dict()
    }


    symptom_dict = {
        **symptom_map.set_index('symptom')['qc_symptom'].to_dict()
    }
    symptom_dict = {
        k.lower():v for k,v in symptom_dict.items() if not pd.isnull(k)
    }

    symptom_id_dict = {
        **symptom_map.set_index('qc_symptom')['symptom_id'].to_dict()
    }
    symptom_id_dict = {
        k.lower():v for k,v in symptom_id_dict.items() if not pd.isnull(k)
    }
    print(symptom_id_dict)


    clinical_split_top5 = clinical_url_top5["symptom_merge"] \
        .str.split('|', expand=True) \
        .stack().reset_index(level=1, drop=True) \
        .rename('symptom') \
        .str.strip() \
        .to_frame() \
        .merge(clinical_url_top5, left_index=True, right_index=True) \
        .drop_duplicates() \
        .rename(columns=columns) \
        .replace({
            '/': np.nan
        }) \
        .dropna(subset=['qc_date']) \
        .assign(
            text_patient=lambda x: x.text_patient.str.strip(),
            text_patient_en=lambda x: x.text_patient_en.str.strip(),
            description=lambda x: x.description.str.strip(),
            clinical_text_en=lambda x: x.clinical_text_en.str.strip(),
            qc_date=lambda x: pd.to_datetime(x.qc_date),
            qc_symptom = lambda x: x.symptom.str.lower().map(symptom_dict),
            symptom_id = lambda x: x.qc_symptom.str.lower().map(symptom_id_dict),
            new_case_id = lambda x: x.case_id.map(caseid_dict)
        ) \
        .dropna(subset=['qc_symptom'])

    clinical_split_wiki = clinical_url_wiki["symptom_merge"] \
        .str.split('|', expand=True) \
        .stack().reset_index(level=1, drop=True) \
        .rename('symptom') \
        .str.strip() \
        .to_frame() \
        .merge(clinical_url_wiki, left_index=True, right_index=True) \
        .drop_duplicates() \
        .rename(columns=wiki_columns) \
        .replace({
            '/': np.nan
        }) \
        .dropna(subset=['qc_date']) \
        .assign(
            text_patient=lambda x: x.text_patient.str.strip(),
            description=lambda x: x.description.str.strip(),
            qc_date=lambda x: pd.to_datetime(x.qc_date,errors='ignore'),
            qc_symptom = lambda x: x.symptom.str.lower().map(symptom_dict),
            symptom_id = lambda x: x.qc_symptom.str.lower().map(symptom_id_dict),
            new_case_id = lambda x: x.case_id.map(caseid_dict)
        ) \
        .dropna(subset=['symptom','symptom_id'])

    article_choose = article_clinical[article_clinical.PMID==31978945]

    article_new_case_id = pd.DataFrame(['C3197894501','C3197894501','C3197894501','C3197894502','C3197894502','C3197894502','C3197894502','C3197894503'])
    article_virus_id = pd.DataFrame(['OEAV000250','OEAV000250','OEAV000250','OEAV000251','OEAV000251','OEAV000251','OEAV000251','OEAV000252'])

    article_choose_new = article_choose.assign(
        url = lambda x: 'https://pubmed.ncbi.nlm.nih.gov/' + x.PMID.astype('str'),
        new_case_id = article_new_case_id,
        virus_id = article_virus_id
    )

    article_choose_split = article_choose_new['症状'] \
        .str.split('|', expand=True) \
        .stack().reset_index(level=1, drop=True) \
        .rename('symptom') \
        .str.strip() \
        .to_frame() \
        .merge(article_choose_new, left_index=True, right_index=True) \
        .drop_duplicates() \
        .rename(columns=article_columns) \
        .replace({
            '-': np.nan
        }) \
        .dropna(subset=['qc_date']) \
        .assign(
            text_patient=lambda x: x.text_patient.str.strip(),
            description=lambda x: x.description.str.strip(),
            qc_date=lambda x: pd.to_datetime(x.qc_date),
            qc_symptom = lambda x: x.symptom.str.lower().map(symptom_dict),
            symptom_id = lambda x: x.qc_symptom.str.lower().map(symptom_id_dict)
        ) \
        .dropna(subset=['qc_symptom']) \
        .drop(article_drop_columns,axis=1)

    drop_columns = ['case_id','PTD']    
    concat_list = [clinical_split_top5,clinical_split_wiki,article_choose_split]
    clinical_merge = pd.concat(concat_list,ignore_index=True)
    clinical_drop = clinical_merge.drop(columns=drop_columns,axis=1)
    clinical_drop.to_csv(
        CLINICAL,
        sep='\t',
        index=False
    )

    print(clinical_drop.shape)
    
    node_symptom = symptom_map[['qc_symptom','symptom_id']] \
        .drop_duplicates()

    node_symptom.to_csv(
            NODE_SYMPTOM,
            sep='\t',
            encoding='utf-8',
            index=False
        )
    print('sucess')

if __name__ == '__main__':
    build()


