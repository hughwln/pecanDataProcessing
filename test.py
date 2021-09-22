'''
Function:
    1.check the content of output files
author: Yi Hu(yhu28@ncsu.edu)
date: Sept. 2021
'''

import pandas as pd
import pickle
import os
from os import walk

file_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Reconstructed\\'
for (dirpath, dirnames, filenames) in walk(file_path):
    for file in filenames:
        if file != '1042':
            continue
        fullname = os.path.join(dirpath, file)
        with open(fullname, 'rb') as f:
            data = pickle.load(f)
            # print(data)

file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\1min\\1042'
with open(file_name, 'rb') as f:
    data = pickle.load(f)
    print(data)

file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\15min\\1042'
with open(file_name, 'rb') as f:
    data = pickle.load(f)
    print(data)

file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\water\\1042'
with open(file_name, 'rb') as f:
    data = pickle.load(f)
    print(data)

file_name = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\PecanStreet_Separately\\gas\\1042'
with open(file_name, 'rb') as f:
    data = pickle.load(f)
    print(data)