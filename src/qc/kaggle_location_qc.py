import numpy as np
import pandas as pd


def get_kaggle_location(city,province,country):
#     print(city,province)
    if pd.isnull(city):
        if pd.isnull(province):
            if pd.isnull(country):
                location = None
                return location
                print('case 1',location)
            elif pd.notnull(country):
                location = None
                return location
                print('case 2',location)

        elif pd.notnull(province):
            if pd.isnull(country):
                location = province
                return location
                print('case 3',location)
            elif pd.notnull(country):
                location = f'{province},{country}'
                return location
                print('case 4',location)
    elif pd.notnull(city):
        if pd.isnull(province):
            if pd.isnull(country):
                location = city
                return location
                print('case 5',location)
            elif pd.notnull(country):
                location = f'{city},{country}'
                return location
                print('case 6',location)
        elif pd.notnull(province):
            if pd.isnull(country):
                location = f'{city},{province}'
                return location
                print('case 7',location)
            elif pd.notnull(country):
                location = f'{city},{province},{country}'
                return location
                print('case 8',location)
    else:
        print('not found',city,province)

# def get_kaggle_location(city,province):
# #     print(city,province)
#     if city == None:
#         if province == None:
#             location = None
#             return location
#             print('case 1',location)
#         elif province != None:
#             location = province
#             return location
#             print('case 2',location)
#     elif city != None:
#         if province == None:
#             location = city
#             return location
#             print('case 3',location)
#         elif province != None:
#             location = f'{province}|{city}'
#             return location
#             print('case 4',location)
#     else:
#         print('not found',city,province)


# def get_kaggle_location(city,province):
# #     print(city,province)
    
#     if city == None:
#         if province == None:
#             location = None
#             return location
#             print('case 1',location)
#         elif province != None:
#             location = f'{province}'
#             return location
#             print('case 2',location)
#     elif city != None:
#         if province == None:
#             location = f'Unknown|{city}'
#             return location
#             print('case 3',location)
#         elif province != None:
#             location = f'{province}|{city}'
#             return location
#             print('case 4',location)
#     else:
#         print('not found',city,province)
#     if province == None:
#         if city == None:
#             location = None
#             return location
#             print('case 5',location)
#         elif city != None:
#             location = f'Unknown|{city}'
#             return location
#     elif province != None:
#         if city == None:
#             location = f'{province}'
#             return location
#             print('case 6',location)
#         elif city != None:
#             locatoin = f'{province}|{city}'
#             return location
        
#     else:
#         print('not found',city,province)
print(get_kaggle_location(None,None,None))