'''
Function:
    1.sort by timestamp, fill missing data with NaN
    2.merge different kinds of data into one file for each user
author: Yi Hu(yhu28@ncsu.edu)
date: Sept. 2021
'''

import csv
import os
from os import walk
import pandas as pd
import numpy as np
import pickle

def save_1min_list():
    load_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\1min\\'
    users_1min = []
    for (dirpath, dirnames, filenames) in walk(load_path):
        for file in filenames:
            fullname = os.path.join(dirpath, file)
            users_1min.append(int(file))
            with open(fullname, 'rb') as inf:
                print(fullname)
                output_data = pickle.load(inf)
                output_data = output_data.sort_index()
                output_data.index = pd.to_datetime(output_data.index, utc=True)
                output_data = output_data.loc[~output_data.index.duplicated(), :]

                # fill missing rows
                start_t = output_data.index[0]
                end_t = output_data.index[-1]
                g = pd.date_range(start=start_t, end=end_t, freq='T')
                output_data = output_data.reindex(g)
                # print(g)

                # rename the index
                output_data.index.name = 'Time'
                # print(output_data)

                # with open(fullname, 'wb') as outf:
                #     pickle.dump(output_data, outf)

    users_1min.sort()

    user_list = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\complete\\1min_user_list.csv'
    with open(user_list, 'w', newline='') as ul:
        writer = csv.writer(ul)
        for user in users_1min:
            writer.writerow([user])

def save_15min_list():
    load_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\15min\\'
    users_15min = []
    for (dirpath, dirnames, filenames) in walk(load_path):
        for file in filenames:
            # if file == '-1':
            #     continue

            fullname = os.path.join(dirpath, file)
            users_15min.append(int(file))
            # with open(fullname, 'rb') as inf:
            #     print(fullname)
            #     output_data = pickle.load(inf)
            #     output_data = output_data.sort_index()
            #
            #     output_data.index = pd.to_datetime(output_data.index, utc=True)
            #     output_data = output_data.loc[~output_data.index.duplicated(), :]
            #
            #     # fill missing rows
            #     start_t = output_data.index[0]
            #     end_t = output_data.index[-1]
            #     g = pd.date_range(start=start_t, end=end_t, freq='15T')
            #     # print(g)
            #     output_data = output_data.reindex(g)
            #     # print(output_data)
            #     # rename the index
            #     output_data.index.name = 'Time'
            #     # print(output_data)
            #
            #     with open(fullname, 'wb') as outf:
            #         pickle.dump(output_data, outf)

    users_15min.sort()
    print(users_15min)

    # user_list = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\complete\\15min_user_list.csv'
    # with open(user_list, 'w', newline='') as ul:
    #     writer = csv.writer(ul)
    #     for user in users_15min:
    #         writer.writerow([user])

def save_water_list():
    load_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\water\\'
    users_water = []
    for (dirpath, dirnames, filenames) in walk(load_path):
        for file in filenames:
            fullname = os.path.join(dirpath, file)
            users_water.append(int(file))
            # output_data = {}
            with open(fullname, 'rb') as inf:
                print(fullname)
                output_data = pickle.load(inf)
                output_data = output_data.sort_index()
                output_data.index = pd.to_datetime(output_data.index, utc=True)
                output_data = output_data.loc[~output_data.index.duplicated(), :]
                # rename the index
                output_data.index.name = 'Time'

                with open(fullname, 'wb') as outf:
                    pickle.dump(output_data, outf)

                # print(output_data)

    users_water.sort()
    print(users_water)
    # user_list = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\complete\\water_user_list.csv'
    # with open(user_list, 'w', newline='') as ul:
    #     writer = csv.writer(ul)
    #     for user in users_water:
    #         writer.writerow([user])

def save_gas_list():
    load_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\gas\\'
    users_gas = []
    for (dirpath, dirnames, filenames) in walk(load_path):
        for file in filenames:
            fullname = os.path.join(dirpath, file)
            users_gas.append(int(file))
            # output_data = {}
            with open(fullname, 'rb') as inf:
                print(fullname)
                output_data = pickle.load(inf)
                output_data = output_data.sort_index()
                output_data.index = pd.to_datetime(output_data.index, utc=True)
                output_data = output_data.loc[~output_data.index.duplicated(), :]
                # rename the index
                output_data.index.name = 'Time'

                with open(fullname, 'wb') as outf:
                    pickle.dump(output_data, outf)

                # print(output_data)

    users_gas.sort()
    print(users_gas)
    # user_list = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\complete\\gas_user_list.csv'
    # with open(user_list, 'w', newline='') as ul:
    #     writer = csv.writer(ul)
    #     for user in users_gas:
    #         writer.writerow([user])

def load_data(file_name):
    if os.path.isfile(file_name):
        with open(file_name, 'rb') as inf:
            data = pickle.load(inf).sort_index()
            return data
    else:
        return pd.DataFrame()

def mergeData():
    path_1min = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\1min\\'
    users = []
    for (dirpath, dirnames, filenames) in walk(path_1min):
        for file in filenames:
            users.append(int(file))
    path_15min = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\15min\\'
    for (dirpath, dirnames, filenames) in walk(path_15min):
        for file in filenames:
            users.append(int(file))
    path_water = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\water\\'
    for (dirpath, dirnames, filenames) in walk(path_water):
        for file in filenames:
            users.append(int(file))
    path_gas = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\gas\\'
    for (dirpath, dirnames, filenames) in walk(path_gas):
        for file in filenames:
            users.append(int(file))

    users.sort()
    users_s = pd.Series(users)
    users_s = users_s.drop_duplicates()
    # print(users_s.head(20).to_string())

    output_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\complete\\'

    for i in users_s.index:
        data = {}
        userid = users_s[i]
        data['userid'] = userid
        output_file = output_path + str(userid)
        print(output_file)
        # if userid != 585:
        #     continue

        file_name_1min = path_1min + str(userid)
        file_name_15min = path_15min + str(userid)
        file_name_water = path_water + str(userid)
        file_name_gas = path_gas + str(userid)

        data['1min'] = load_data(file_name_1min)
        data['15min'] = load_data(file_name_15min)
        data['water'] = load_data(file_name_water)
        data['gas'] = load_data(file_name_gas)

        # print(data)

        # with open(output_file, 'wb') as outf:
        #     pickle.dump(data, outf)

# save_1min_list()
# save_15min_list()
# save_water_list()
# save_gas_list()
mergeData()
