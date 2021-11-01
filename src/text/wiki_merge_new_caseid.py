import rootpath
rootpath.append()

import pandas as pd
import numpy as np
from src.qc.new_case_id import split_join
from src.utils.data_file import filepath
from src.qc.mkdir import mkdir
from src.qc.wiki_location_qc import get_wiki_location
pd.set_option('display.max_columns', None)

# INPUT
WIKIPEDIA = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','covid19流行病信息汇总-20200515.xlsx')
FRANCE = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','COVID-19 France.xls')
GERMANY = filepath(rootpath.detect(),'data/sync/KGCOV/raw/manual','COVID-19 Germany.xlsx')


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/output/text')
mkdir(OUTPUT_DIR)
WIKI_MERGE_NEW = filepath(OUTPUT_DIR,'wiki_merge_new_caseid.tsv')

def build():
    columns = {
            '来源': 'source_detail', 
            '国籍': 'nationality', 
            '国家': 'country', 
            '地区': 'location', 
            '报道日期': 'collection_date', 
            '病例确诊日期': 'confirmed_date',
            '病例年龄': 'age', 
            '病例性别': 'gender', 
            '病例接触信息': 'contact_history', 
            '流行病学资料': 'travel_history',
            '原文': 'text', 
            '在该地区是否为首例病例': 'is_first_case'
        }


    df_wiki_choose = pd.read_excel(
        WIKIPEDIA,
        usecols=columns.keys()
    ) \
        .rename(columns=columns) \
        .assign(
            source_detail=lambda x: x.source_detail.replace({
                '文本': 'wiki_text',
                '表格': 'wiki_table'
            }),
            is_first_case=lambda x: x.is_first_case.replace({
                '是': 'yes',
                '否': 'no'
            }),
            url=lambda x: 'https://en.wikipedia.org/wiki/COVID-19_pandemic_in_'+x.country,
            source='wiki',
            original_ID=lambda x: x.index,
            collection_date=lambda x: x[['collection_date', 'confirmed_date']] \
                .fillna(method='bfill', axis=1)['collection_date'],
            location = lambda x: x.apply(lambda x: get_wiki_location(x.location,x.country),axis=1)
        )


            
    columns = {
            '国家': 'country', 
            '地区': 'location', 
            '报道日期': 'collection_date',
            '年龄': 'age', 
            '性别': 'gender', 
            '流行病学资料': 'travel_history', 
            '原文': 'text',
        }

    df_france_choose = pd.read_excel(
        FRANCE,
        usecols=columns.keys()
    ) \
        .rename(columns=columns) \
        .assign(
            original_ID=lambda x: x.index,
            url='https://en.wikipedia.org/wiki/COVID-19_pandemic_in_France',
            source='wiki',
            location = lambda x: x.apply(lambda x: get_wiki_location(x.location,x.country),axis=1)
        )
            

    df_germany_choose = pd.read_excel(
        GERMANY,
        usecols=columns.keys()
    ) \
        .rename(columns=columns) \
        .assign(
            original_ID=lambda x: x.index,
            url='https://en.wikipedia.org/wiki/COVID-19_pandemic_in_Germany',
            source='wiki',
            location = lambda x: x.apply(lambda x: get_wiki_location(x.location,x.country),axis=1)
        )

            
    df_wiki_list = [df_france_choose,df_germany_choose,df_wiki_choose]
    df_wiki_merge = pd.concat(df_wiki_list,ignore_index=True)
    df_wiki_merge['new_case_id'] = pd.Series([
            f'C{str(i+1200000000).zfill(10)}'
            for i in range(df_wiki_merge.shape[0])
        ])
    df_wiki_merge.country[df_wiki_merge.new_case_id=='C1200000874'] = 'Georgia'
    
    df_wiki_merge.to_csv(
                WIKI_MERGE_NEW,
                sep='\t',
                encoding='utf-8',
                index=False
            )

    print(df_wiki_merge[df_wiki_merge.new_case_id=='C1200000874'])
    print(df_wiki_merge.shape)


if __name__ == '__main__':
    build()