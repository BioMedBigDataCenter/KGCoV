def position(position,var_aa):
    if '-' in position:
        location = str(position).split('-')
        zoom_strat = int(location[0])
        zoom_end = int(location[1])
        if '-' in var_aa:
            interval = str(var_aa).split('-')
            var_start = int(interval[0])
            var_end = int(interval[1])
            if (zoom_strat<var_start and zoom_end>var_end):
                return True
            else:
                return False
        else:
            var = int(var_aa)
            if zoom_strat<var and zoom_end>var:
                return True
            else:
                return False