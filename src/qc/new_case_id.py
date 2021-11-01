def split_join(original_id):
    original = str(original_id).split('-')
    if len(original) == 3:
        merge_id = ''.join(original[1:3])
        last_id = merge_id.zfill(7)
        first_id = original[0].zfill(3)
        new_case_id = ''.join(['C',first_id,last_id])
        return new_case_id
    elif len(original) == 2:
        merge_id = original[-1]
        last_id = merge_id.zfill(7)
        first_id = original[0].zfill(3)
        new_case_id = ''.join(['C',first_id,last_id])
        return new_case_id