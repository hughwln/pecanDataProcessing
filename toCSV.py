'''
Function:
    1.convert 1min data into CSV file, set time zone as local
author: Yi Hu(yhu28@ncsu.edu)
date: Sept. 2021
'''

import csv
import pandas as pd
import pickle
import os
from os import walk
import us

'''
compare user list in 1min data and meta data file
'''
def check_userlist():
    list_1min = []
    list_meta = pd.Series(dtype=int)
    load_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min\\'
    for (dirpath, dirnames, filenames) in walk(load_path):
        for file in filenames:
            list_1min.append(int(file))
    list_1min = pd.Series(list_1min, dtype=int)

    meta_file = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\metadata-2021-09.csv'
    with open(meta_file, 'rb') as mf:
        metadata = pd.read_csv(mf)
        metadata = metadata.iloc[1:]
        list_meta = pd.Series(metadata['dataid'], dtype=int)

    list_1min = list_1min.sort_values()
    list_meta = list_meta.sort_values()

    in1min_nometa = list_1min[~list_1min.isin(list_meta)]
    inmeta_no1min = list_meta[~list_meta.isin(list_1min)]
    both = list_1min[list_1min.isin(list_meta)]

    in1min_nometa.name = 'userid'
    inmeta_no1min.name = 'userid'
    only1minfile = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min_users_not_in_meta.csv'
    onlymetafile = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\meta_users_not_in_1min.csv'
    in1min_nometa.to_csv(only1minfile, index=False)
    inmeta_no1min.to_csv(onlymetafile, index=False)

def toCSV():
    meta_file = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\metadata-2021-09.csv'
    with open(meta_file, 'rb') as mf:
        metadata = pd.read_csv(mf)
        metadata = metadata.iloc[1:]
        meta_users = pd.Series(metadata['dataid'], dtype=str)
        # print(meta_users)

    # g = pd.date_range(start=start_t, end=end_t, freq='T')
    # pd.DatetimeIndex(
    daylight_saving_days = [['2012-1-1 00:00:00', '2012-3-11 00:00:00'],
                            ['2012-3-12 00:00:00', '2012-11-4 00:00:00'],
                            ['2012-11-5 00:00:00', '2013-3-10 00:00:00'],
                            ['2013-3-11 00:00:00', '2013-11-3 00:00:00'],
                            ['2013-11-4 00:00:00', '2014-3-9 00:00:00'],
                            ['2014-3-10 00:00:00', '2014-11-2 00:00:00'],
                            ['2014-11-3 00:00:00', '2015-3-8 00:00:00'],
                            ['2015-3-9 00:00:00', '2015-11-1 00:00:00'],
                            ['2015-11-2 00:00:00', '2016-3-13 00:00:00'],
                            ['2016-3-14 00:00:00', '2016-11-6 00:00:00'],
                            ['2016-11-7 00:00:00', '2017-3-12 00:00:00'],
                            ['2017-3-13 00:00:00', '2017-11-5 00:00:00'],
                            ['2017-11-6 00:00:00', '2018-3-11 00:00:00'],
                            ['2018-3-12 00:00:00', '2018-11-4 00:00:00'],
                            ['2018-11-5 00:00:00', '2019-3-10 00:00:00'],
                            ['2019-3-11 00:00:00', '2019-11-3 00:00:00'],
                            ['2019-11-4 00:00:00', '2020-3-8 00:00:00'],
                            ['2020-3-9 00:00:00', '2020-11-1 00:00:00'],
                            ['2020-11-2 00:00:00', '2021-3-14 00:00:00'],
                            ['2021-3-15 00:00:00', '2021-11-7 00:00:00'],
                            ['2021-11-8 00:00:00', '2022-1-1 00:00:00']]
    state_list = ['New York', 'Texas', 'California', 'Colorado']

    load_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min\\'
    output_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\validUsers\\1min_csv\\'
    appliances = pd.Series(dtype=str)
    cities = []
    for (dirpath, dirnames, filenames) in walk(load_path):
        for file in filenames:
            if file not in meta_users.values:
                continue
            # print(file)
            output_file = output_path + file + '.csv'
            city = metadata.loc[metadata['dataid'] == file, 'state'].iloc[0]
            if city not in cities:
                cities.append(city)
            time_zone = us.states.lookup(city).capital_tz
            print(file, ' ', city, ' ', time_zone)

            fullname = os.path.join(dirpath, file)
            with open(fullname, 'rb') as inf:
                input_data = pickle.load(inf)
                appliances = appliances.append(pd.Series(input_data.columns)).drop_duplicates()
                # print(appliances)
                # convert time zone
                input_data.index = input_data.index.tz_convert(time_zone)
                output_data = pd.DataFrame()
                for i in daylight_saving_days:
                    output_data = output_data.append(input_data.loc[i[0]:i[1]])
                # buff = input_data.loc[pd.to_datetime(daylight_saving_days[10]):pd.to_datetime(daylight_saving_days[11])]
                # print(buff)
                # print(output_data)

                # write to csv
                output_data.to_csv(output_file)


    appliances = appliances.drop_duplicates()
    appliances.to_csv('C:\\Users\\yhu28\\Downloads\\outputPecanData\\validUsers\\1min_csv\\appliances.csv', index=False)
    # print(appliances)
    # user_list = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\complete\\1min_user_list.csv'
    #     with open(user_list, 'w', newline='') as ul:
    #         writer = csv.writer(ul)
    #         for user in users_1min:
    #             writer.writerow([user])

def deleteinvalidusers():
    meta_file = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\metadata-2021-09.csv'
    with open(meta_file, 'rb') as mf:
        metadata = pd.read_csv(mf)
        metadata = metadata.iloc[1:]
        meta_users = pd.Series(metadata['dataid'], dtype=str)

    path_1min = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min\\'
    path_15min = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\15min\\'
    path_water = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\water\\'
    path_gas = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\gas\\'
    path_com = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Reconstructed\\Data\\'

    # delete 1min files
    users_1min = []
    for (dirpath, dirnames, filenames) in walk(path_1min):
        for file in filenames:
            if file in meta_users.values:
                # print('append: ', file, 'type: ', type(file))
                users_1min.append(int(file))
            else:
                src = path_1min + file
                dst = path_1min + 'd' + file
                os.rename(src, dst)
                # print(src)
                print(dst)
    users_1min = pd.Series(users_1min, dtype=int)
    users_1min.name = 'userid'
    out_1min = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min_users_list_valid.csv'
    users_1min.to_csv(out_1min, index=False)

    # delete 15min files
    users_15min = []
    for (dirpath, dirnames, filenames) in walk(path_15min):
        for file in filenames:
            if file in meta_users.values:
                # print('append: ', file, 'type: ', type(file))
                users_15min.append(int(file))
            else:
                src = path_15min + file
                dst = path_15min + 'd' + file
                os.rename(src, dst)
                # print(src)
                print(dst)
    users_15min = pd.Series(users_15min, dtype=int)
    users_15min.name = 'userid'
    out_15min = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\15min_users_list_valid.csv'
    users_15min.to_csv(out_15min, index=False)

    # delete water files
    users_water = []
    for (dirpath, dirnames, filenames) in walk(path_water):
        for file in filenames:
            if file in meta_users.values:
                # print('append: ', file, 'type: ', type(file))
                users_water.append(int(file))
            else:
                src = path_water + file
                dst = path_water + 'd' + file
                os.rename(src, dst)
                # print(src)
                print(dst)
    users_water = pd.Series(users_water, dtype=int)
    users_water.name = 'userid'
    out_water = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\water_users_list_valid.csv'
    users_water.to_csv(out_water, index=False)

    # delete gas files
    users_gas = []
    for (dirpath, dirnames, filenames) in walk(path_gas):
        for file in filenames:
            if file in meta_users.values:
                # print('append: ', file, 'type: ', type(file))
                users_gas.append(int(file))
            else:
                src = path_gas + file
                dst = path_gas + 'd' + file
                os.rename(src, dst)
                # print(src)
                print(dst)
    users_gas = pd.Series(users_gas, dtype=int)
    users_gas.name = 'userid'
    out_gas = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\gas_users_list_valid.csv'
    users_gas.to_csv(out_gas, index=False)

    # delete complete data
    users = []
    for (dirpath, dirnames, filenames) in walk(path_com):
        for file in filenames:
            if file in meta_users.values:
                # print('append: ', file, 'type: ', type(file))
                users.append(int(file))
            else:
                src = path_com + file
                dst = path_com + 'd' + file
                os.rename(src, dst)
                # print(src)
                print(dst)
    users = pd.Series(users, dtype=int)
    users.name = 'userid'
    out_f = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Reconstructed\\users_list_valid.csv'
    users.to_csv(out_f, index=False)

# check_userlist()
# toCSV()
deleteinvalidusers()