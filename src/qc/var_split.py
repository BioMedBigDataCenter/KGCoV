def get_var(var_aa):
    try:
        var_aa = var_aa.split(',')[0]
        return var_aa
    except AttributeError:
        var_aa = None
        return var_aa