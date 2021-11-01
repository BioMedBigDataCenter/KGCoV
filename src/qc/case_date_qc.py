from datetime import datetime
import time
import re
import pandas as pd 

# 校正collection_date，全部变成datetime.
# 把日期全变成年月日的顺序


def parse_date(collection_date):
    success_flag = False
    if '.' in collection_date:
        pattern = r'(\d+)\.(\d+)\.2020'
        res = re.match(pattern, str(collection_date).strip(), re.S)
        if res != None:
            date = datetime(2020, int(res.group(2)), int(res.group(1)))
            success_flag = True
            return date 
            print('case 5',date)

    elif '-' in collection_date:
        for pattern in [r'2020\-(\d+)\-(\d+)\s\d+\:',r'2020\-(\d+)\-(\d+)']:
            res = re.match(pattern, str(collection_date).strip(), re.S)
            if res != None:
                date = datetime(2020, int(res.group(1)), int(res.group(2)))
                success_flag = True
                return date 
                print('case 6',date)

    elif '/' in collection_date:
        for pattern in [r'2020\/(\d+)\/(\d+)', r'(\d+)\/(\d+)\/2020']:
            res = re.match(pattern, str(collection_date).strip(), re.S)
            if res != None:
                date = datetime(2020, int(res.group(1)), int(res.group(2)))
                success_flag = True
                return date 
                print('case 7',date)
                break
    
    if not success_flag:
        raise ValueError('Wrong date: '+str(collection_date))
        return date
        print('case 7',collection_date)


# 分割日期，分割之后只有一个的按上面的函数校正；有两个的取平均值
def get_datetime(case_date):
    if '2020' not in case_date:
        actual_date = pd.to_datetime(case_date)
        return actual_date
        print('case 1',actual_date)
    casedate_seg = str(case_date).strip().split('-')
    if len(casedate_seg) == 1:
        actual_date = parse_date(casedate_seg[0])
        return actual_date
        print('case 2',actual_date)
    elif len(casedate_seg) == 2:
        fromdate, todate = parse_date(casedate_seg[0]), parse_date(casedate_seg[1])
        actual_date = (todate-fromdate)/2 + fromdate
        return actual_date
        print('case 3',actual_date)
    elif len(casedate_seg) == 3:
        actual_date = parse_date(case_date)
        return actual_date
        print('case 4',actual_date)
    else:
        actual_date = None
        return actual_date
        raise ValueError('Wrong date: '+str(case_date))
    
print(get_datetime('06.03.2020 - 10.03.2020'))

print(parse_date('06.03.2020'))

