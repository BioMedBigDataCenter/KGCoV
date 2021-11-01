def get_genome_data_source(xref_id):
    if 'EPI' in xref_id:
        data_source = 'GISAID'
        return data_source
    else:
        data_source = 'GenBank'
        return data_source