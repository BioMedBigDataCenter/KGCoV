import rootpath
rootpath.append()

from lxml import etree
from bs4 import BeautifulSoup as bs
import requests
import pandas
import json
from openpyxl import Workbook

from src.utils.data_file import filepath


# OUTPUT
OUTPUT_DIR = filepath(rootpath.detect(),'data/sync/KGCOV/raw/generated')
FEATURE_DICT = filepath(OUTPUT_DIR,'sars2.xlsx')

columns = ['primaryAccession','Name','Gene','Type','Description','location_start','location_end', \
    'Position','evidenceCode','source','id','label','label_text','source_url']

def build():
    def get_response(url):
        response = requests.get(url)
        if response.status_code==200:
            response_data = json.loads(response.text)
            return response_data
        return

    def parse_html(response, labelInfo, sourceInfo):
        data = {}

        data["name"] = response.get("primaryAccession")
        strList = []
        genes = response.get("genes", [])
        if genes:
            for i in genes:
                for k, v in i.items():
                    try:
                        strList.append(f"{k}:{v[0].get('value')}")
                    except Exception as e:
                        strList.append(f"{k}:{v.get('value')}")
        try:
            data["name_"] = response.get("proteinDescription").get("recommendedName").get("fullName").get("value")
        except Exception:
            data["name_"] = ""

        data["genes"] = "\n".join(strList)

        for feature in response.get("features"):
            typeName = feature.get("type")
            #if typeName in typeList:
            data["typeName"] = typeName
            data["description"] = feature.get("description")
            data["location_start"] = feature.get("location").get("start").get("value")
            data["location_end"] = feature.get("location").get("end").get("value")
            data["location"] = f"{data['location_start']}-{data['location_end']}"
            evidences = feature.get("evidences")
            if evidences:
                for evidence in evidences:
                    data["evidenceCode"] = evidence.get("evidenceCode")
                    data["source"] = evidence.get("source", "")
                    data["id"] = evidence.get("id", "")
                    data["label"] = labelInfo.get(data["evidenceCode"])[0]
                    data["label_content"] = labelInfo.get(data["evidenceCode"])[1]
                    if data["source"] in list(sourceInfo.keys()):
                        data["source_url"] = sourceInfo.get(data["source"]).format(data["id"])
                    else:
                        data["source_url"] = ""
            else:
                data["evidenceCode"] = ""
                data["source"] = ""
                data["id"] = ""
                data["label"] = ""
                data["label_content"] = ""
                data["source_url"] = ""

            yield data


    if __name__ == '__main__':
        wb = Workbook()
        wb.create_sheet("data", 0)
        ws = wb.get_sheet_by_name("data")
        ws.append(columns)

        protein_id = ["P0DTC2","P0DTC1", "P0DTD1", "P0DTC7", "P0DTC4", "P0DTD2",
                    "P0DTC3", "P0DTC5", "P0DTD3", "P0DTD8","P0DTC6", "P0DTC9", "P0DTD8"]
        for id in protein_id:
            url = 'https://www.ebi.ac.uk/uniprot/api/covid-19/uniprotkb/accession/{}'.format(id)

            _label = {
                "ECO:0000255": ["By similarity", "Manual assertion inferred from sequence similarity"],
                "ECO:0000250": ["automatic annotation", "Automatic assertion according to rules"],
                "ECO:0000305": ["curated", "Manual assertion inferred from experiment"],
                "ECO:0000269": ["publication", "Manual assertion based on experiment"],
                "ECO:0000244": ["Combined sources", "Manual assertion inferred from combination of experimental and computational evidence"]
            }
            _source_url = {
                "PROSITE-ProRule":"https://prosite.expasy.org/rule/{}",
                "UniProtKB":"https://www.uniprot.org/uniprot/{}",
                "PubMed":"{}",
                "HAMAP-Rule": "https://hamap.expasy.org/rule/{}",
                "PDB":"https://www.ebi.ac.uk/pdbe/entry/pdb/{}"
            }
            response = get_response(url)
            for data in parse_html(response, _label, _source_url):
                data = list(data.values())
                print(data)
                print('-'*20)
                ws.append(data)
        

        wb.save(FEATURE_DICT)
        print("sucess")

if __name__ == '__main__':
    build()