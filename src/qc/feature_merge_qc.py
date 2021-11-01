import pandas as pd 

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