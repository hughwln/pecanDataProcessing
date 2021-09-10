import csv
import pandas as pd
import numpy as np
import pickle
import os
from os import walk
import sys
import datetime

def read_from_local(file_name, str, chunk_size=500000):
    print(' Loading ' + file_name + '...')
    reader = pd.read_csv(file_name, header=0, iterator=True, index_col=str)
    chunks = []
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)
            chunks.append(chunk)
        except StopIteration:
            loop = False

    df_ac = pd.concat(chunks)
    print(' Finished! data_len:', len(df_ac.index))
    return df_ac

def process_elec_1min(file):
    users = {}

    print(' Loading ' + file, ' ', datetime.datetime.now())
    temp_read = pd.read_csv(file, header=0, iterator=True, index_col='localminute')
    temp = temp_read.get_chunk(10)
    for col in range(1, len(temp.columns)):
        temp.iloc[:, col] = pd.to_numeric(temp.iloc[:, col], downcast='float')

    # create the dict of index names and optimized datatypes
    d_types = temp.dtypes
    col_names = d_types.index
    types = [i.name for i in d_types.values]
    column_types = dict(zip(col_names, types))

    reader = pd.read_csv(file, header=0, iterator=True, index_col='localminute', dtype=column_types)
    chunk_size = 500000
    loop = True
    while loop:
        try:
            data = reader.get_chunk(chunk_size)
            user_list = data.dataid.drop_duplicates()
            for user in user_list:
                group = data[data['dataid'] == user]
                # print(group.sort_index())
                if user in users:
                    users[user] = users[user].append(group.iloc[:, 1:])
                else:
                    users[user] = group.iloc[:, 1:]

        except StopIteration:
            loop = False

    # delete NaN columns
    for u in users.keys():
        users[u] = users[u].dropna(axis=1, how='all')

    # store data
    print(' Store ' + file, ' ', datetime.datetime.now())
    # print(sys.getsizeof(users))
    # print(users)
    output_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\1min\\'
    for u in users.keys():
        # print(sys.getsizeof(users[u]))
        output_file_name = output_path + str(u)
        if os.path.isfile(output_file_name):
            # load file and append
            # output_data = {}
            with open(output_file_name, 'rb') as inf:
                output_data = pickle.load(inf)
                output_data = output_data.append(users[u])

                with open(output_file_name, 'wb') as outf:
                    # Pickle the 'data' dictionary using the highest protocol available.
                    # print(output_data)
                    pickle.dump(output_data, outf) # , pickle.HIGHEST_PROTOCOL)
        else:
            # cread new file
            output_data = users[u]
            with open(output_file_name, 'wb') as outf:
                # Pickle the 'data' dictionary using the highest protocol available.
                pickle.dump(output_data, outf)
        # print(users[u].sort_index())

def search_elec_1min():
    # get file paths and names
    filepath = 'C:\\Users\\yhu28\\Downloads\\Pecan_Street_full_dataset_20210813\\electricity\\1min_real\\'

    for (dirpath, dirnames, filenames) in walk(filepath):
        # f.extend(dirpath + dirnames + filenames)
        for file in filenames:
            fullname = os.path.join(dirpath, file)
            # print(fullname)
            process_elec_1min(fullname)
    # process_elec_1min('C:\\Users\\yhu28\\Downloads\\Pecan_Street_full_dataset_20210813\\electricity\\1min_real\\1min_real2012\\one_min_real2012_aug.csv')
    # process_elec_1min('C:\\Users\\yhu28\\Downloads\\Pecan_Street_full_dataset_20210813\\electricity\\1min_real\\1min_real2012\\one_min_real2012_april.csv')


def process_elec_15min(file):
    users = {}
    # data = read_from_local(file, 'local_15min')
    print(' Loading ' + file, ' ', datetime.datetime.now())

    temp_read = pd.read_csv(file, header=0, iterator=True, index_col='local_15min')
    temp = temp_read.get_chunk(10)
    for col in range(1, len(temp.columns)):
        temp.iloc[:, col] = pd.to_numeric(temp.iloc[:, col], downcast='float')

    # create the dict of index names and optimized datatypes
    d_types = temp.dtypes
    col_names = d_types.index
    types = [i.name for i in d_types.values]
    column_types = dict(zip(col_names, types))

    reader = pd.read_csv(file, header=0, iterator=True, index_col='local_15min', dtype=column_types)
    chunk_size = 500000
    loop = True
    while loop:
        try:
            data = reader.get_chunk(chunk_size)
            user_list = data.dataid.drop_duplicates()
            for user in user_list:
                group = data[data['dataid'] == user]
                # print(group.sort_index())
                if user in users:
                    users[user] = users[user].append(group.iloc[:, 1:])
                else:
                    users[user] = group.iloc[:, 1:]

        except StopIteration:
            loop = False

    # delete NaN columns
    for u in users.keys():
        users[u] = users[u].dropna(axis=1, how='all')

    # store data
    print('Store ' + file, ' ', datetime.datetime.now())
    output_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\15min\\'
    for u in users.keys():
        output_file_name = output_path + str(u)
        if os.path.isfile(output_file_name):
            # load file and append
            # print('     update existing file: ' + str(u))
            # output_data = {}
            with open(output_file_name, 'rb') as inf:
                output_data = pickle.load(inf)
                output_data = output_data.append(users[u])

                with open(output_file_name, 'wb') as outf:
                    # Pickle the 'data' dictionary using the highest protocol available.
                    # print(output_data)
                    pickle.dump(output_data, outf)
        else:
            # cread new file
            # print('     creat new file: ' + str(u))
            output_data = users[u]
            with open(output_file_name, 'wb') as outf:
                # Pickle the 'data' dictionary using the highest protocol available.
                pickle.dump(output_data, outf)
        # print(users[u].sort_index())

def search_elec_15min():
    # get file paths and names
    filepath = 'C:\\Users\\yhu28\\Downloads\\Pecan_Street_full_dataset_20210813\\electricity\\15min_real\\'

    for (dirpath, dirnames, filenames) in walk(filepath):
        # f.extend(dirpath + dirnames + filenames)
        for file in filenames:
            fullname = os.path.join(dirpath, file)
            # print(fullname)
            process_elec_15min(fullname)
    # process_elec_15min('C:\\Users\\yhu28\\Downloads\\Pecan_Street_full_dataset_20210813\\electricity\\15min_real\\15min_real_2012.csv')


def process_water():
    users = {}
    file = 'C:\\Users\\yhu28\\Downloads\\Pecan_Street_full_dataset_20210813\\water_ert.csv'
    # data = read_from_local(file, 'local_15min')
    print(' Loading ' + file, ' ', datetime.datetime.now())

    temp_read = pd.read_csv(file, header=0, iterator=True, index_col='readtime')
    temp = temp_read.get_chunk(10)
    for col in range(1, len(temp.columns)):
        temp.iloc[:, col] = pd.to_numeric(temp.iloc[:, col], downcast='float')

    # create the dict of index names and optimized datatypes
    d_types = temp.dtypes
    col_names = d_types.index
    types = [i.name for i in d_types.values]
    column_types = dict(zip(col_names, types))

    reader = pd.read_csv(file, header=0, iterator=True, index_col='readtime', dtype=column_types)
    chunk_size = 500000
    loop = True
    while loop:
        try:
            data = reader.get_chunk(chunk_size)
            user_list = data.dataid.drop_duplicates()
            for user in user_list:
                group = data[data['dataid'] == user]
                # print(group.sort_index())
                if user in users:
                    users[user] = users[user].append(group.iloc[:, 1:])
                else:
                    users[user] = group.iloc[:, 1:]

        except StopIteration:
            loop = False

    # delete NaN columns
    for u in users.keys():
        users[u] = users[u].dropna(axis=1, how='all')

    # store data
    print('Store ' + file, ' ', datetime.datetime.now())
    output_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\water\\'
    for u in users.keys():
        output_file_name = output_path + str(u)
        if os.path.isfile(output_file_name):
            # load file and append
            # print('     update existing file: ' + str(u))
            # output_data = {}
            with open(output_file_name, 'rb') as inf:
                output_data = pickle.load(inf)
                output_data = output_data.append(users[u])

                with open(output_file_name, 'wb') as outf:
                    # Pickle the 'data' dictionary using the highest protocol available.
                    # print(output_data)
                    pickle.dump(output_data, outf)
        else:
            # cread new file
            # print('     creat new file: ' + str(u))
            output_data = users[u]
            with open(output_file_name, 'wb') as outf:
                # Pickle the 'data' dictionary using the highest protocol available.
                pickle.dump(output_data, outf)
        # print(users[u].sort_index())

def process_gas():
    users = {}
    file = 'C:\\Users\\yhu28\\Downloads\\Pecan_Street_full_dataset_20210813\\gas_ert.csv'
    # data = read_from_local(file, 'local_15min')
    print(' Loading ' + file, ' ', datetime.datetime.now())

    temp_read = pd.read_csv(file, header=0, iterator=True, index_col='readtime')
    temp = temp_read.get_chunk(10)
    for col in range(1, len(temp.columns)):
        temp.iloc[:, col] = pd.to_numeric(temp.iloc[:, col], downcast='float')

    # create the dict of index names and optimized datatypes
    d_types = temp.dtypes
    col_names = d_types.index
    types = [i.name for i in d_types.values]
    column_types = dict(zip(col_names, types))

    reader = pd.read_csv(file, header=0, iterator=True, index_col='readtime', dtype=column_types)
    chunk_size = 500000
    loop = True
    while loop:
        try:
            data = reader.get_chunk(chunk_size)
            user_list = data.dataid.drop_duplicates()
            for user in user_list:
                group = data[data['dataid'] == user]
                # print(group.sort_index())
                if user in users:
                    users[user] = users[user].append(group.iloc[:, 1:])
                else:
                    users[user] = group.iloc[:, 1:]

        except StopIteration:
            loop = False

    # delete NaN columns
    for u in users.keys():
        users[u] = users[u].dropna(axis=1, how='all')

    # store data
    print('Store ' + file, ' ', datetime.datetime.now())
    output_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\gas\\'
    for u in users.keys():
        output_file_name = output_path + str(u)
        if os.path.isfile(output_file_name):
            # load file and append
            # print('     update existing file: ' + str(u))
            # output_data = {}
            with open(output_file_name, 'rb') as inf:
                output_data = pickle.load(inf)
                output_data = output_data.append(users[u])

                with open(output_file_name, 'wb') as outf:
                    # Pickle the 'data' dictionary using the highest protocol available.
                    # print(output_data)
                    pickle.dump(output_data, outf)
        else:
            # cread new file
            # print('     creat new file: ' + str(u))
            output_data = users[u]
            with open(output_file_name, 'wb') as outf:
                # Pickle the 'data' dictionary using the highest protocol available.
                pickle.dump(output_data, outf)
        # print(users[u].sort_index())

def perform():
    # elec_1min = search_elec_1min()
    # elec_15min = search_elec_15min()
    water = process_water()
    gas = process_gas()

    data = water

    return data