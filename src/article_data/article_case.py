import rootpath
rootpath.append()
from src.utils.data_file import filepath
from time import sleep
import numpy as np
import pandas as pd
import re
from src.qc.mkdir import mkdir


# INPUT
INPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/graph_data')
NODE_CASE = filepath(INPUT_DIR,'NODE_Case_new.tsv')
TRAVEL = filepath(INPUT_DIR,'LINK_TRAVEL_CaseTravelInfo.tsv')
CONTACT = filepath(INPUT_DIR,'LINK_CONTACT_CaseContactInfo.tsv')
CLINICAL = filepath(INPUT_DIR,'LINK_PRESENT_CaseClinicalInfo.tsv')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/target')
mkdir(OUTPUT_DIR)
ARTICLE_CASE = filepath(OUTPUT_DIR, 'case.tsv')


case_columns = ['case_id','gender','qc_gender','age','qc_age','case_reported_date','qc_case_reported_date','qc_city','qc_province', \
                'country','qc_country','location','qc_location', \
                'qc_continent','count','rank','percentile', \
               'data_source','description','url','contactinfo','travelinfo','clinicalinfo']

case_rename_columns = {'new_case_id':'case_id','qc_date':'qc_case_reported_date','date':'case_reported_date'}

case_drop_columns = ['qc_location_id','original_ID']

def build():
    case = pd.read_csv(
        NODE_CASE,
        sep='\t',
        encoding='utf-8'
    )

    print(case.columns)
    print(case.shape)


    # 在article_case中加上travel contact clinical的info信息
    travel = pd.read_csv(
        TRAVEL,
        sep='\t',
        encoding='utf-8', 
        usecols=['qc_date','from_location','transport','to_location','new_case_id']
    )

    contact = pd.read_csv(
        CONTACT,
        sep='\t',
        encoding='utf-8',
        usecols=['qc_date','new_contact_case_id','new_case_id']
    )

    clinical = pd.read_csv(
        CLINICAL,
        sep='\t',
        encoding='utf-8',
        usecols=['symptom_merge','new_case_id']
    )

    # travel merge
    travel_drop = travel.drop_duplicates(subset=['new_case_id'])
    travel_drop.reset_index(drop=True,inplace=True)

    travel_drop.rename(columns={'qc_date':'date','new_case_id':'case_id'},inplace=True)

    travel_chooose = travel_drop[['date','from_location','transport','to_location']]
    result = []
    travel_list=[]

    for ri,row in travel_chooose.iterrows():
        travelinfo = row.to_dict()
        result.append(travelinfo)
        travel_list.append(result)

    travel_data = pd.DataFrame(travel_list)
    travel_info = pd.DataFrame(travel_data.iloc[0])

    travel_info.columns = ['travelinfo']
    travel_caseid = travel_drop[['case_id']]
    travel_merge = travel_caseid.merge(travel_info,right_index=True,left_index=True,how='left')

    # concat merge
    contact_drop = contact.dropna(subset=['new_contact_case_id']).drop_duplicates(subset=['new_case_id'])
    contact_drop.rename(columns={'qc_date':'date','new_case_id':'case_id','new_contact_case_id':'contact_case_id'},inplace=True)
    contact_drop.reset_index(drop=True,inplace=True)

    contact_chooose = contact_drop[['date','contact_case_id']]
    result = []
    contact_list=[]

    for ri,row in contact_chooose.iterrows():
        contactinfo = row.to_dict()
        result.append(contactinfo)
        contact_list.append(result)
        
        
    contact_data = pd.DataFrame(contact_list)
    contact_info = pd.DataFrame(contact_data.iloc[0])

    contact_info.columns = ['contactinfo']

    contact_caseid = contact_drop[['case_id']]

    contact_merge = contact_caseid.merge(contact_info,right_index=True,left_index=True,how='left')

    # symptom merge

    clinical_drop = clinical.drop_duplicates(subset='new_case_id')
    clinical_drop.rename(columns={'symptom_merge':'symptom','new_case_id':'case_id'},inplace=True)
    clinical_drop.reset_index(drop=True,inplace=True)

    clinical_choose = clinical_drop[['symptom']]


    result = []
    clinical_list=[]

    for ri,row in clinical_choose.iterrows():
        clinicalinfo = row.to_dict()
        result.append(clinicalinfo)
        clinical_list.append(result)
        
        
    clinical_data = pd.DataFrame(clinical_list)
    clinical_info = pd.DataFrame(clinical_data.iloc[0])

    clinical_info.columns = ['clinicalinfo']

    clinical_caseid = clinical_drop[['case_id']]

    clinical_merge = clinical_caseid.merge(clinical_info,right_index=True,left_index=True,how='left')


    case_replace = case.replace({'kaggle':'beoutbreakprepared'})

    case_replace.rename(columns=case_rename_columns,inplace=True)
    case_drop = case_replace.drop(case_drop_columns,axis=1)

    case_travel = case_drop.merge(travel_merge,on='case_id',how='left').drop_duplicates(subset=['case_id'],keep='last')
    case_contact = case_travel.merge(contact_merge,on='case_id',how='left').drop_duplicates(subset=['case_id'])
    case_clinical = case_contact.merge(clinical_merge,on='case_id',how='left').drop_duplicates(subset=['case_id'],keep='last')

    case_adjust = case_clinical[case_columns]
    print(case_adjust.shape)
    print(case_adjust.columns)
    case_adjust.to_csv(
        ARTICLE_CASE,
        sep='\t',
        encoding='utf-8',
        index=False
    )


if __name__ == '__main__':
    build()