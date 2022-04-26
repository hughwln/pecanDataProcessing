import csv
import pandas as pd
import numpy as np
# import pickle
# import os
# from os import walk
# import sys
import datetime
import re

def isthesame(s1, s2):
    if s1[-1] == ' ' and len(s1) > 1:
        s1 = s1[:-1]
    if s2[-1] == ' ' and len(s2) > 1:
        s2 = s2[:-1]
    #  columns[i] in columns[j] and 
    if s1 == s2:
        return True
    else:
        return False

def merge_index(arg):
    columns = np.sort(arg)
    # print(columns)
    index = {}
    for i in range(len(columns)-1):
        if columns[i] in index:
            continue
        end = min(i+5, len(columns))
        for j in range(i+1, end):
            s1 = re.split('[:(]', columns[i])
            s2 = re.split('[:(]', columns[j])
            
            l = min(len(s1), len(s2))
            if len(s1) == len(s2):
                continue
            if l > 3 and not isthesame(s1[l-1], s2[l-1]):
                continue
            
            if len(s1) < 3 or len(s2) < 3:
                continue
            if s1[2][-1] == ' ' and len(s1[2]) > 1:
                s1[2] = s1[2][:-1]
            if s2[2][-1] == ' ' and len(s2[2]) > 1:
                s2[2] = s2[2][:-1]
            #  columns[i] in columns[j] and 
            if s1[0] == s2[0] and s1[2] == s2[2] and s1[1][-2:] == s2[1][-2:]:
                index[columns[j]] = columns[i]
                # print(i, columns[i], j, columns[j])
    # print(index)
    return index
    
def load_data(file):
    temp_read = pd.read_csv(file, header=0, iterator=True)
    temp = temp_read.get_chunk(100)
    data_new = temp.reindex(sorted(temp.columns), axis = 1)
    cols = merge_index(temp.columns)

    
    X = pd.DataFrame()
    datas = []
    reader = pd.read_csv(file, header=0, iterator=True)
    chunk_size = 100
    loop = True
    print('Loading data...')
    time = 1
    while loop:
        try:
            print('No. ', time)
            time += 1
            data = reader.get_chunk(chunk_size)
            print('    Reindex data')
            data = data.reindex(sorted(data.columns), axis = 1)
            print('    Merge data')
            for col in cols:
                base_col = cols[col]
                isDropColumn = True
                for r in data.index:
                    if pd.notnull(data.loc[r, col]):
                        if pd.isnull(data.loc[r, cols[col]]):
                            data.loc[r, cols[col]] = data.loc[r, col]
                        else:
                            print('=========', cols[col], col)
                            isDropColumn = False
                            print("!!!")
                            break
                if isDropColumn:
                    data = data.drop(col, axis=1)
                else:
                    continue
            datas.append(data)
            # print(data.iloc[-1, 1])
        except StopIteration:
            loop = False
        # print(data)
    print('Concat')
    X = pd.concat(datas)
    X = X.dropna(axis=1, how='all')
    print('Load completed')
    X.to_csv('2019_AM_Data-005.csv')
    # print(X)

load_data('C:\\Users\\yhu28\\Downloads\\2019_AM_Data-001.csv')