'''
Function:
    1.check the content of output files
author: Yi Hu(yhu28@ncsu.edu)
date: Sept. 2021
'''

import pandas as pd
import pickle
import numpy as np
import pandas as pd
import os
from os import walk
import matplotlib.pyplot as plt

# file_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Reconstructed\\'
# for (dirpath, dirnames, filenames) in walk(file_path):
#     for file in filenames:
#         if file != '1042':
#             continue
#         fullname = os.path.join(dirpath, file)
#         with open(fullname, 'rb') as f:
#             data = pickle.load(f)
#             # print(data)

def test_9982():
    print('C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min\\9982')
    file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min\\9982'
    with open(file_name, 'rb') as f:
        data = pickle.load(f)
        print(data.loc['2015-08-18 00:00:00-05:00': '2015-08-20 00:00:00-05:00', 'solar'])


    file_1min = 'C:\\Users\\yhu28\\Downloads\\one_min_real2015_aug.csv'
    file_15min = 'C:\\Users\\yhu28\\Downloads\\15min_real_2015.csv'
    print(' Loading ' + file_1min, ' ')
    temp_read = pd.read_csv(file_1min, header=0, iterator=True, index_col='localminute')
    temp = temp_read.get_chunk(10)
    for col in range(1, len(temp.columns)):
        temp.iloc[:, col] = pd.to_numeric(temp.iloc[:, col], downcast='float')

    # create the dict of index names and optimized datatypes
    d_types = temp.dtypes
    col_names = d_types.index
    types = [i.name for i in d_types.values]
    column_types = dict(zip(col_names, types))

    data_1min = pd.DataFrame()
    reader = pd.read_csv(file_1min, header=0, iterator=True, index_col='localminute', dtype=column_types)
    chunk_size = 500000
    loop = True
    while loop:
        try:
            data = reader.get_chunk(chunk_size)
            user_list = data.dataid.drop_duplicates()

            group = data[data['dataid'] == 9982]
            data_1min = data_1min.append(group)

        except StopIteration:
            loop = False

    data_1min.index = pd.to_datetime(data_1min.index, utc=True)
    print(data_1min.loc['2015-08-18 00:00:00-05:00':'2015-08-20 00:00:00-05:00'])
    print(data_1min.loc['2015-08-18 00:00:00-05:00':'2015-08-20 00:00:00-05:00'].columns)

    # ============================================================================================
    print(' Loading ' + file_15min, ' ')
    temp_read = pd.read_csv(file_15min, header=0, iterator=True, index_col='local_15min')
    temp = temp_read.get_chunk(10)
    for col in range(1, len(temp.columns)):
        temp.iloc[:, col] = pd.to_numeric(temp.iloc[:, col], downcast='float')

    # create the dict of index names and optimized datatypes
    d_types = temp.dtypes
    col_names = d_types.index
    types = [i.name for i in d_types.values]
    column_types = dict(zip(col_names, types))

    data_15min = pd.DataFrame()
    reader = pd.read_csv(file_15min, header=0, iterator=True, index_col='local_15min', dtype=column_types)
    chunk_size = 500000
    loop = True
    while loop:
        try:
            data = reader.get_chunk(chunk_size)
            user_list = data.dataid.drop_duplicates()

            group = data[data['dataid'] == 9982]
            data_15min = data_15min.append(group)

        except StopIteration:
            loop = False

    data_15min.index = pd.to_datetime(data_15min.index, utc=True)
    print(data_15min.loc['2015-08-18 00:00:00-05:00':'2015-08-20 00:00:00-05:00', 'solar'])

def rebuild_77():
    print('C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min\\77')
    file_1min = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min\\77'
    file_15min = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\15min\\77'
    file_gas = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\gas\\77'
    file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Reconstructed\\Data\\77'
    with open(file_1min, 'rb') as f_1min:
        data_1min = pickle.load(f_1min)
    with open(file_15min, 'rb') as f_15min:
        data_15min = pickle.load(f_15min)
    with open(file_gas, 'rb') as f_gas:
        data_gas = pickle.load(f_gas)

    data = {}
    data['userid'] = np.int64(77)
    data['1min'] = data_1min
    data['15min'] = data_15min
    data['water'] = pd.DataFrame()
    data['gas'] = data_gas

    print(data)

    with open(file_name, 'wb') as outf:
        pickle.dump(data, outf)

def load_file(userid):

    file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Reconstructed\\Data\\' + str(userid)
    print(userid)
    with open(file_name, 'rb') as f:
        data = pickle.load(f)
        print(data)

# file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\15min\\1042'
# with open(file_name, 'rb') as f:
#     data = pickle.load(f)
#     print(data)
#
# file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\water\\1042'
# with open(file_name, 'rb') as f:
#     data = pickle.load(f)
#     print(data)
#
# file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\gas\\1042'
# with open(file_name, 'rb') as f:
#     data = pickle.load(f)
#     print(data)

# load_file(77)

def pcc():
    load_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min_csv\\'
    dataset = pd.DataFrame()
    count = 0
    for (dirpath, dirnames, filenames) in walk(load_path):
        for file in filenames:
            fullname = os.path.join(dirpath, file)
            data = pd.read_csv(fullname, index_col='Time')

            # data = data.loc['2020-04-05 00:00:00-04:00':'2020-05-16 23:59:00-04:00']
            # data = data.loc['2017-07-08 00:00:00-04:00':'2017-08-18 23:59:00-04:00']
            data = data.loc['2018-07-08 00:00:00-04:00':'2018-08-18 23:59:00-04:00']
            if len(data.index) == 0:
                continue
            # print(file, data)

            if 'grid' in data.columns:
                # print('data', data)
                dataset[file[:-4]] = data['grid']
                dataset = dataset.dropna(axis=1, how='any')
                print(dataset)
                count = len(dataset.columns)
                if count > 99:
                    break
    outputfile = 'C:\\Users\\yhu28\\Documents\\Code\\Data\\pecandata2018.csv'
    dataset.to_csv(outputfile)

def pcc1():
    file = 'C:\\Users\\yhu28\\Documents\\Code\\Data\\pecandata2018.csv'
    data = pd.read_csv(file, index_col='Time')

    # dt = pd.date_range(start='2020-04-05 00:00:00-04:00', end='2020-05-16 23:59:00-04:00', freq='1min')
    # dt = pd.date_range(start='2017-07-08 00:00:00-04:00', end='2017-08-18 23:58:00-04:00', freq='1min')
    dt = pd.date_range(start='2018-07-08 00:00:00-04:00', end='2018-08-18 23:58:00-04:00', freq='1min')

    mask = dt.minute % 15 == 0

    data = data[mask]

    print(data)
    R = data.corr(method='pearson')
    print(R)
    # R = R.reshape(-1, 1)
    # plt.hist(R, bins=30)
    # plt.xlabel('PCC(PecanStreetData)')
    # plt.ylabel('N')
    # plt.show()
    R['city'] = pd.Series()
    R = R.append(pd.Series(name='city'))

    meta_file = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\metadata-2021-09.csv'
    with open(meta_file, 'rb') as mf:
        metadata = pd.read_csv(mf, index_col='dataid')
        metadata = metadata.iloc[1:, 2:4]
        #
        # cors = pd.DataFrame(columns=['pcc', 'user1', 'user2', 'city1', 'city2'])
        for i in range(100):
            R.iloc[i, -1] = metadata.loc[R.index[i], 'city']
        for j in range(100):
            R.iloc[-1, j] = metadata.loc[R.columns[j], 'city']

    print(R)
    # R.to_csv('C:\\Users\\yhu28\\Documents\\Code\\Data\\pccMatrix20200405-20200516.csv')
    # R.to_csv('C:\\Users\\yhu28\\Documents\\Code\\Data\\pccMatrix20170708-20170818.csv')
    # R.to_csv('C:\\Users\\yhu28\\Documents\\Code\\Data\\pccMatrix20180708-20180818.csv')

    data = data.to_numpy()
    X = data.reshape((-1, 96*7, 100))
    for i in range(6):
        R = np.corrcoef(X[i], rowvar=False)
        print(R.shape, R)
        R = R.reshape(-1, 1)
        plt.title('mean=%f    ' % np.mean(R) + 'std=%f' % np.std(R))
        plt.hist(R, bins=30)
        plt.xlabel('PCC(PecanStreetData)')
        plt.ylabel('N')
        plt.show()
        # plt.pcolormesh(X[i])
        # plt.show()

def test():
    df1 = pd.DataFrame([[1,2],[3,4]], index=[1,2], columns=[0,1])
    df2 = pd.DataFrame([[10, 20], [30, 40]], index=[0, 2], columns=[2, 3])
    df3 = pd.concat([df1, df2], axis=1)
    print(df3)

test()
#
# data = pd.read_csv('C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min_csv\\35.csv', index_col='Time')
# print(data)
# data = data.loc['2021-07-17 00:00:-05:00':'2021-07-18 00:00:00-05:00']
# print(data)
# data = data.dropna(axis=1, how='all')