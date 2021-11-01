import rootpath
rootpath.append()
from src.utils.data_file import filepath
import pandas as pd
import numpy as np
import re
from src.qc.mkdir import mkdir


# INPUT
CONTACT_XLSX = filepath(rootpath.detect(), 'data/sync/KGCOV/raw/manual', '接触史结构化-0630.xlsx')
TOP5 = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','top5-二次审编完成.csv')
CASEID_DICT = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated','caseid_dict.tsv')
WIKI_CONTACT = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','Wiki-接触史结构化合并.xlsx')
WIKI_MANUAL = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','Wiki-数据清洗合并.xlsx')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
LINK_CONTACT_CaseContactInfo = filepath(OUTPUT_DIR,'LINK_CONTACT_CaseContactInfo.tsv')

def build():
    columns = {
        'PTD': 'case_id',
        'contact_text': 'description',
        '什么时间': 'qc_date',
        '在哪': 'location',
        '干什么': 'situation',
        '和谁': 'ref_text',
        '与接触对象关系': 'relationship',
        '接触对象virus_id': 'contact_virus_id',
        '接触对象PTD': 'contact_case_id'
    }

    wiki_columns = {
        'contact_text': 'description',
        '什么时间': 'qc_date',
        '在哪': 'location',
        '干什么': 'situation',
        '和谁': 'ref_text',
        '与接触对象关系': 'relationship',
        '接触对象virus_id': 'contact_virus_id'
    }

    drop_columns = ['能否明确接触对象']
    drop_columns_wiki = ['接触对象PTD','PTD']

    contact_top5 = pd.read_excel(
        CONTACT_XLSX
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

    contact_wiki = pd.read_excel(
        WIKI_CONTACT
        ) \
        .assign(
        case_id = lambda x: x.PTD.replace(r'PT.',r'C',regex=True),
        contact_case_id = lambda x: x['接触对象PTD'].replace(r'PT.',r'C',regex=True)
        )
 
    url_wiki = pd.read_excel(
        WIKI_MANUAL,
        usecols=['PTD','source_wiki']
    ) \
        .assign(
            case_id = lambda x: x.PTD.replace(r'PT.',r'C',regex=True),
            url = lambda x: x.source_wiki
        )[['case_id','url']]

    caseid = pd.read_csv(
    CASEID_DICT,
    sep='\t',
    encoding='utf-8'
    )

    caseid_dict = {
        **caseid.set_index('case_id')['new_case_id'].to_dict()
    }

    contact_drop_top5 = contact_top5.drop(drop_columns, axis=1) \
        .rename(
            columns=columns
        ) \
        .replace({
            '/': ''
        }) \
        .assign(
            text_patient=lambda x: x.text_patient.str.strip(),
            text_patient_en=lambda x: x.text_patient_en.str.strip(),
            description=lambda x: x.description.str.strip(),
            contact_text_en=lambda x: x.contact_text_en.str.strip(),
            qc_date=lambda x: pd.to_datetime(x.qc_date),
            new_case_id = lambda x: x.case_id.map(caseid_dict),
            new_contact_case_id = lambda x: x.contact_case_id.map(caseid_dict)
        ) \
        .merge(url_top5, on='case_id', how='left') \
        .drop_duplicates() \
        .drop(['Unnamed: 14'],axis=1)

    contact_drop_wiki = contact_wiki.drop(drop_columns, axis=1) \
        .rename(
            columns=wiki_columns
        ) \
        .replace({
            '/': ''
        }) \
    .assign(
        text_patient=lambda x: x.text_patient.str.strip(),
        description=lambda x: x.description.str.strip(),
        qc_date=lambda x: pd.to_datetime(x.qc_date,errors='ignore')
    ) \
    .merge(url_wiki, on='case_id', how='left') \
    .drop_duplicates()
    contact_drop_wiki['new_contact_case_id'] = contact_drop_wiki['contact_case_id'].map(caseid_dict)
    contact_drop_wiki['new_case_id'] = contact_drop_wiki['case_id'].map(caseid_dict)

    drop_columns = ['case_id','contact_case_id','PTD','接触对象PTD']
    concat_list = [contact_drop_top5,contact_drop_wiki]
    contact_merge = pd.concat(concat_list,ignore_index=True) \
        .drop(columns=drop_columns,axis=1) \
        .dropna(subset=['new_case_id'])
    
    contact_merge.to_csv(
            LINK_CONTACT_CaseContactInfo,
            sep='\t',
            encoding='utf-8',
            index=False
        )

    print(contact_merge.shape)
    print('sucess')

if __name__ == '__main__':
    build()
