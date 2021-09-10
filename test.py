import pandas as pd
import pickle
import os
from os import walk

file_path = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\complete\\'
for (dirpath, dirnames, filenames) in walk(file_path):
    for file in filenames:
        fullname = os.path.join(dirpath, file)
        with open(fullname, 'rb') as f:
            data = pickle.load(f)
            print(data)