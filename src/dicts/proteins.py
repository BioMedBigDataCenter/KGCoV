import rootpath
rootpath.append()

from src.utils.data_file import filepath
from src.utils.network import get_csv, urlencode
from src.qc.mkdir import mkdir

# PUTPUT
OUTPUT_DIR = filepath(rootpath.detect(), 'data/sync/KGCOV/output/graph_data')
mkdir(OUTPUT_DIR)
NODE_TSV = filepath(OUTPUT_DIR, 'NODE_Protein(CV).tsv')

VIGTK_ENTRY_MAP = {
    'R1A_SARS2': 'ORF1a_polyprotein',
    'SPIKE_SARS2': 'surface_glycoprotein',
    'R1AB_SARS2': 'ORF1ab_polyprotein',
    'AP3A_SARS2': 'ORF3a_protein',
    'VME1_SARS2': 'membrane_glycoprotein',
    'NS7A_SARS2': 'ORF7a_protein',
    'NCAP_SARS2': 'nucleocapsid_phosphoprotein',
    'VEMP_SARS2': 'envelope_protein',
    'NS6_SARS2': 'ORF6_protein',
    'ORF9B_SARS2': None,
    'A0A663DJA2_SARS2': 'ORF10_protein',
    'NS8_SARS2': 'ORF8_protein',
    'Y14_SARS2': None,
    'NS7B_SARS2': 'ORF7b_protein',
}

UNIPROT_COLUMN_MAP = {
    'Entry': 'entry',
    'Entry Name': 'entry_name',
    'Protein names': 'protein_names',
    'Gene Names': 'gene_names',
    'Organism': 'organism_name',
    'Organism (ID)': 'organism',
    'Binding site': 'binding_site',
    'Catalytic activity': 'catalytic_activity',
    'Domain [FT]': 'domain',
    'Region': 'region',
    'Motif': 'motif',
}

uniprot_csv_url = urlencode(
    'https://www.ebi.ac.uk/uniprot/api/covid-19/uniprotkb/stream', {
        'compressed': 'true',
        'format': 'tsv',
        'query': '*',  # AND (reviewed:true)
        'fields': ','.join([
            'accession', 'id', 'protein_name', 'gene_names', 'organism_name', 'organism_id',
            'ft_binding', 'cc_catalytic_activity', 'ft_domain', 'ft_region', 'ft_motif'
        ])
    })



def build():
    df, _ = get_csv(uniprot_csv_url, sep='\t')

    df.assign(
        vigtk_alias=lambda x: x['Entry Name'].map(VIGTK_ENTRY_MAP)
    ).rename(columns=UNIPROT_COLUMN_MAP).to_csv(
        NODE_TSV, sep='\t', index=False, encoding='utf-8'
    )
    print('sucess')


if __name__ == '__main__':
    build()
