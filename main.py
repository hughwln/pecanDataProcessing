# This is a sample Python script.
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import csv
import numpy
import pandas as pd
import numpy as np
import pickle
import splitUser


outputPath = 'C:\\Users\\yhu28\\Downloads\\outputPecanData\\'

def get_users():
    with open('C:\\Users\\yhu28\\Downloads\\Pecan_Street_full_dataset_20210813\\metadata.csv') as metafile:
        metaData = pd.read_csv(metafile)
        # print(metaData)
        user = metaData.iloc[1:, 0]
        # print(user)
        users = np.array(user, dtype=np.int64)
        return users

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    data = splitUser.perform()

        # file_name = outputPath + str(userID) + '.pickle'
        # with open(file_name, 'wb') as outf:
        #     # Pickle the 'data' dictionary using the highest protocol available.
        #     pickle.dump(data, outf, pickle.HIGHEST_PROTOCOL)

