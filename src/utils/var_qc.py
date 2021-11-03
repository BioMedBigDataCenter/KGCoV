import pandas as pd
import numpy as np

def get_var(var_aa):
    try:
        var_aa = var_aa.split(',')[0]
        return var_aa
    except AttributeError:
        var_aa = None
        return var_aa

def change_var(synonymous,var_aa,var_aa_pos):
    if synonymous == 'synonymous SNV':
        var_aa = var_aa.split('-')[0]
        if len(str(var_aa_pos)) != 1:
            var_pos = str(var_aa_pos).split(',')[0]
            var_aa_id = var_aa + var_pos + var_aa
            return var_aa_id
        elif len(str(var_aa_pos)) == 1:
            var_aa_id = var_aa + var_aa_pos + var_aa
            return var_aa_id
        else:
            raise ValueError
    elif synonymous == 'nonsynonymous SNV':
        var_aa_ori = var_aa.split('-')[0]
        var_aa_change = var_aa.split('-')[1]
        if len(str(var_aa_pos)) != 1:
            var_pos = str(var_aa_pos).split(',')[0]
            var_aa_id = var_aa_ori + var_pos + var_aa_change
            return var_aa_id
        elif len(str(var_aa_pos)) == 1:
            var_aa_id = var_aa_ori + var_aa_pos + var_aa_change
            return var_aa_id
        else:
            raise ValueError
    else:
        return np.nan

def concat_func(x):
    dupli_ids = x[x.feature_id.duplicated()]['feature_id'].tolist()
    for feature_id in x['feature_id']:
        if feature_id in dupli_ids:
            return pd.Series({
                'type':'|'.join(x['type'].unique()),
                'description':'|'.join(x['description'].unique())
            })
        else:
            return pd.Series({
                'type': x['type'].unique()[0],
                'description':x['description'].unique()[0]
            })
