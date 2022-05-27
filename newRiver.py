import pandas as pd
import pickle
import os
from os import walk
import numpy as np

class newRiverLoader():

    def __init__(self):
        self.smids = [['74684774', '74684775', '74685050', '74715007', '74715009', '74715075', '74715076', '74715078'],
                      ['74684542', '74715516', '74715517', '74715518', '74715739', '74715740', '74715741', '74715748'],
                      ['74713715', '74713716', '74713717', '74713718', '74713760', '74713771', '74713772', '74713773'],
                      ['74684959', '74684960', '74714511', '74714599', '74714600', '74714601', '74717163', '74717164'],
                      ['74714539', '74714540', '74714541', '74714542', '74714607', '74714608', '74714609', '74714610'],
                      ['74714580', '74714582', '74714774', '74716552', '74716596', '74716664', '74716665', '75044146'],
                      ['74715841', '74715842', '74716696', '74716697', '74716784', '74716785', '74716786', '74717216'],
                      ['74685466', '74685467', '74685483', '74685497', '74685521', '74685545', '74685546', '74685547']]
        self.dfs = [pd.DataFrame(columns=self.smids[0]),
                    pd.DataFrame(columns=self.smids[1]),
                    pd.DataFrame(columns=self.smids[2]),
                    pd.DataFrame(columns=self.smids[3]),
                    pd.DataFrame(columns=self.smids[4]),
                    pd.DataFrame(columns=self.smids[5]),
                    pd.DataFrame(columns=self.smids[6]),
                    pd.DataFrame(columns=self.smids[7])]

    def load_data(self):
        filepath = 'C:\\Users\\yhu28\\Downloads\\BadSMsFixedv2\\'
        for (dirpath, dirnames, filenames) in walk(filepath):
            if 'figs' in dirpath:
                continue
            if 'probSMs' in dirpath:
                continue
            if 'removedSMsFixed' in dirpath:
                continue
            for file in filenames:
                if file[-3:] != 'pkl':
                    continue
                if file[3:11] in self.smids[0]:
                    txid = 0
                elif file[3:11] in self.smids[1]:
                    txid = 1
                elif file[3:11] in self.smids[2]:
                    txid = 2
                elif file[3:11] in self.smids[3]:
                    txid = 3
                elif file[3:11] in self.smids[4]:
                    txid = 4
                elif file[3:11] in self.smids[5]:
                    txid = 5
                elif file[3:11] in self.smids[6]:
                    txid = 6
                elif file[3:11] in self.smids[7]:
                    txid = 7
                else:
                    continue

                fullname = os.path.join(dirpath, file)
                with open(fullname, 'rb') as f:
                    data = pickle.load(f)
                    data = data.loc['2017-7-24':'2020-12-27']
                    self.dfs[txid][file[3:11]] = data['usage']

        for i in range(len(self.dfs)):
            self.dfs[i] = self.dfs[i].loc[:, self.dfs[i].mean().sort_values(ascending=True).index]
            self.dfs[i].columns = range(8)

    def groupDataset(self):
        dataset = pd.concat(self.dfs)

        trainDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\GANData.csv'
        dataset.to_csv(trainDataFile)

    def nnDataset(self, day_offset=0, shuffle='origin'):
        time = {0: ['2017-7-24', '2019-12-29', '2019-12-30', '2020-12-20'],
                1: ['2017-7-25', '2019-12-30', '2019-12-31', '2020-12-21'],
                2: ['2017-7-26', '2019-12-31', '2020-1-1', '2020-12-22'],
                3: ['2017-7-27', '2020-1-1', '2020-1-2', '2020-12-23'],
                4: ['2017-7-28', '2020-1-2', '2020-1-3', '2020-12-24'],
                5: ['2017-7-29', '2020-1-3', '2020-1-4', '2020-12-25'],
                6: ['2017-7-30', '2020-1-4', '2020-1-5', '2020-12-26']}
        # training set
        dfs_training = [df.loc[time[day_offset][0]: time[day_offset][1]] for df in self.dfs]
        for i in range(len(dfs_training)):
            # ============shuffle columns============
            if shuffle == 'random':
                dfs_training[i] = dfs_training[i].sample(n=8, axis='columns')
                dfs_training[i].columns = range(8)
            elif shuffle == 'sort':
                dfs_training[i] = dfs_training[i].loc[:, dfs_training[i].mean().sort_values(ascending=True).index]
                dfs_training[i].columns = range(8)
            # ============================================
            dfs_training[i]['label'] = pd.Series(1, index=dfs_training[i].index)

        # negative samples
        for i in range(8):
            fake_group = pd.DataFrame(columns=range(8))
            for c in range(8):
                fake_group[c] = dfs_training[c].iloc[:, (i+c) % 8]
            # ==========shuffle columns=================
            if shuffle == 'random':
                fake_group = fake_group.sample(n=8, axis='columns')
                fake_group.columns = range(8)
            elif shuffle == 'sort':
                fake_group = fake_group.loc[:, fake_group.mean().sort_values(ascending=True).index]
                fake_group.columns = range(8)
            # ============================================
            fake_group['label'] = pd.Series(0, index=fake_group.index)
            dfs_training.append(fake_group)
        training_set = pd.concat(dfs_training)

        # testing set
        dfs_testing = [df.loc[time[day_offset][2]: time[day_offset][3]] for df in self.dfs]
        for i in range(len(dfs_testing)):
            # ==============shuffle columns==============
            if shuffle == 'random':
                dfs_testing[i] = dfs_testing[i].sample(n=8, axis='columns')
                dfs_testing[i].columns = range(8)
            elif shuffle == 'sort':
                dfs_testing[i] = dfs_testing[i].loc[:, dfs_testing[i].mean().sort_values(ascending=True).index]
                dfs_testing[i].columns = range(8)
            # ============================================
            dfs_testing[i]['label'] = pd.Series(1, index=dfs_testing[i].index)
        # negative samples
        for i in range(8):
            fake_group = pd.DataFrame(columns=range(8))
            for c in range(8):
                fake_group[c] = dfs_testing[c].iloc[:, (i+c) % 8]
            # ============= shuffle columns ==============
            if shuffle == 'random':
                fake_group = fake_group.sample(n=8, axis='columns')
                fake_group.columns = range(8)
            elif shuffle == 'sort':
                fake_group = fake_group.loc[:, fake_group.mean().sort_values(ascending=True).index]
                fake_group.columns = range(8)
            # ============================================
            fake_group['label'] = pd.Series(0, index=fake_group.index)
            dfs_testing.append(fake_group)
        testing_set = pd.concat(dfs_testing)

        if shuffle == 'random':
            trainDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\classiferTrain'+str(day_offset)+'.csv'
            testDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\classiferTest'+str(day_offset)+'.csv'
        elif shuffle == 'sort':
            trainDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\classiferTrainSorted' + str(
                day_offset) + '.csv'
            testDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\classiferTestSorted' + str(
                day_offset) + '.csv'
        elif shuffle == 'origin':
            trainDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\classiferTrainOrigin' + str(
                day_offset) + '.csv'
            testDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\classiferTestOrigin' + str(
                day_offset) + '.csv'
        else:
            print('!!! unknown shuffle !!!')

        training_set.to_csv(trainDataFile)
        testing_set.to_csv(testDataFile)

if __name__ == '__main__':
    dataloader = newRiverLoader()
    dataloader.load_data()
    for i in range(7):
        dataloader.nnDataset(i, shuffle='origin')
    # dataloader.groupDataset()