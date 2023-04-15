def convert_date(x):
    if '/' in x:
        ele = x.split('/')
    else:
        ele = x.split('-')
    if len(ele[0]) == 4:
        return ele[0] + '-' + ele[1] + '-' + ele[2]
    return ele[2] + '-' + ele[1] + '-' + ele[0]

def convert_vnd_to_dollar(x):
    return int(x) / 25000