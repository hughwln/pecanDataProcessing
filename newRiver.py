import pandas as pd
import pickle
import os
from os import walk
import numpy as np
import random
import matplotlib.pyplot as plt

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

    def groupDataset(self, day_offset=0):
        time = {0: ['2017-7-24', '2019-12-29', '2019-12-30', '2020-12-20'],
                1: ['2017-7-25', '2019-12-30', '2019-12-31', '2020-12-21'],
                2: ['2017-7-26', '2019-12-31', '2020-1-1', '2020-12-22'],
                3: ['2017-7-27', '2020-1-1', '2020-1-2', '2020-12-23'],
                4: ['2017-7-28', '2020-1-2', '2020-1-3', '2020-12-24'],
                5: ['2017-7-29', '2020-1-3', '2020-1-4', '2020-12-25'],
                6: ['2017-7-30', '2020-1-4', '2020-1-5', '2020-12-26']}
        if day_offset == 7:
            datas = []
            for day in range(7):
                # training set
                dfs_training = [df.loc[time[day][0]: time[day][3]] for df in self.dfs]

                t_path = 'C:\\Users\\yhu28\\Documents\\Code\\Data\\new_river\\'
                t_file = t_path + 'temp_Boone.csv'
                d = pd.read_csv(t_file)
                d.index = pd.to_datetime(d["Date"])
                d = d.loc[time[day][0]: time[day][3]]

                for i in range(len(dfs_training)):
                    dfs_training[i] = dfs_training[i].loc[:, dfs_training[i].mean().sort_values(ascending=True).index]
                    dfs_training[i]['Temp'] = d['Temp'].astype(np.float32)

                data = pd.concat(dfs_training)
                datas.append(data)
            dataset = pd.concat(datas)

        else:
            # training set
            dfs_training = [df.loc[time[day_offset][0]: time[day_offset][3]] for df in self.dfs]

            t_path = 'C:\\Users\\yhu28\\Documents\\Code\\Data\\new_river\\'
            t_file = t_path + 'temp_Boone.csv'
            d = pd.read_csv(t_file)
            d.index = pd.to_datetime(d["Date"])
            d = d.loc[time[day_offset][0]: time[day_offset][1]]

            for i in range(len(dfs_training)):
                dfs_training[i] = dfs_training[i].loc[:, dfs_training[i].mean().sort_values(ascending=True).index]
                dfs_training[i]['Temp'] = d['Temp'].astype(np.float32)

            dataset = pd.concat(dfs_training)

        trainDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\GANData' + str(day_offset) + '.csv'
        dataset.to_csv(trainDataFile)

    def buildNegativeSamples_bymean(self, dfs_training, week_set, num):
        classified_by_mean = [pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame()]

        week_mean = week_set.mean()
        upper = week_mean.max()
        # print(week_mean.max(), week_mean.min())

        # fig = plt.figure()
        # plt.hist(week_mean, bins=6)
        # plt.title('Positive sample mean distribution')
        # plt.show()

        for i in range(len(week_mean)):
            if week_mean.iloc[i] < upper / 6.0:
                classified_by_mean[0][len(classified_by_mean[0].columns)] = week_set.iloc[:, i]
            elif week_mean.iloc[i] < upper * 2.0 / 6.0:
                classified_by_mean[1][len(classified_by_mean[1].columns)] = week_set.iloc[:, i]
            elif week_mean.iloc[i] < upper * 3.0 / 6.0:
                classified_by_mean[2][len(classified_by_mean[2].columns)] = week_set.iloc[:, i]
            elif week_mean.iloc[i] < upper * 4.0 / 6.0:
                classified_by_mean[3][len(classified_by_mean[3].columns)] = week_set.iloc[:, i]
            elif week_mean.iloc[i] < upper * 5.0 / 6.0:
                classified_by_mean[4][len(classified_by_mean[4].columns)] = week_set.iloc[:, i]
            else:
                classified_by_mean[5][len(classified_by_mean[5].columns)] = week_set.iloc[:, i]

        # print(classified_by_mean)
        classified_by_mean_num = [len(df.columns) for df in classified_by_mean]
        max_num = max(classified_by_mean_num)
        max_idx = classified_by_mean_num.index(max_num)
        classified_by_mean_1 = classified_by_mean[max_idx]
        aaa = [classified_by_mean[i] for i in range(len(classified_by_mean)) if i!=max_idx]
        classified_by_mean_2 = pd.concat(aaa, axis=1)

        # temp = []
        for i in range(num):
            a = random.randint(0, 4)
            b = 8 - a
            curves = [classified_by_mean_1.sample(n=a, axis='columns'), classified_by_mean_2.sample(n=b, axis='columns')]
            group = pd.concat(curves, axis=1)
            group.columns = range(8)
            # if len(group.columns) != 8:
            #     print(len(group.columns))
            group = group.loc[:, group.mean().sort_values(ascending=True).index]
            if len(group.columns) == 8:
                group.columns = range(8)
            else:
                print("error!!! not 8 columns")
            group['label'] = pd.Series(0, index=group.index)
            dfs_training.append(group)

        # temp = pd.concat(temp, axis=1)
        # temp = temp.max()
        # fig = plt.figure()
        # plt.hist(temp, bins=6)
        # plt.title('Negative sample mean distribution')
        # plt.show()

    def buildNegativeSamples_bypeak(self, dfs_training, week_set, num):
        classified_by_peak = [pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame(),
                             pd.DataFrame()]

        week_peak = week_set.max()

        upper = week_peak.max()
        # print(week_mean.max(), week_mean.min())

        # fig = plt.figure()
        # plt.hist(week_peak, bins=6)
        # plt.title('Positive sample mean distribution')
        # plt.show()

        for i in range(len(week_peak)):
            if week_peak.iloc[i] < upper / 6.0:
                classified_by_peak[0][len(classified_by_peak[0].columns)] = week_set.iloc[:, i]
            elif week_peak.iloc[i] < upper * 2.0 / 6.0:
                classified_by_peak[0][len(classified_by_peak[0].columns)] = week_set.iloc[:, i]
            elif week_peak.iloc[i] < upper * 3.0 / 6.0:
                classified_by_peak[0][len(classified_by_peak[0].columns)] = week_set.iloc[:, i]
            elif week_peak.iloc[i] < upper * 4.0 / 6.0:
                classified_by_peak[1][len(classified_by_peak[1].columns)] = week_set.iloc[:, i]
            elif week_peak.iloc[i] < upper * 5.0 / 6.0:
                classified_by_peak[1][len(classified_by_peak[1].columns)] = week_set.iloc[:, i]
            else:
                classified_by_peak[1][len(classified_by_peak[1].columns)] = week_set.iloc[:, i]

        # print(classified_by_mean)
        classified_by_peak_num = [len(df.columns) for df in classified_by_peak]
        max_num = max(classified_by_peak_num)
        max_idx = classified_by_peak_num.index(max_num)
        classified_by_peak_1 = classified_by_peak[max_idx]
        aaa = [classified_by_peak[i] for i in range(len(classified_by_peak)) if i!=max_idx]
        classified_by_peak_2 = pd.concat(aaa, axis=1)

        # temp = []
        for i in range(num):
            a = random.randint(0, 5)
            b = 8 - a
            curves = [classified_by_peak_1.sample(n=a, axis='columns'), classified_by_peak_2.sample(n=b, axis='columns')]
            group = pd.concat(curves, axis=1)
            group.columns = range(8)
            # if len(group.columns) != 8:
            #     print(len(group.columns))
            group = group.loc[:, group.mean().sort_values(ascending=True).index]
            if len(group.columns) == 8:
                group.columns = range(8)
            else:
                print("error!!! not 8 columns")
            group['label'] = pd.Series(0, index=group.index)
            dfs_training.append(group)

        # temp = pd.concat(temp, axis=1)
        # temp = temp.max()
        # fig = plt.figure()
        # plt.hist(temp, bins=6)
        # plt.title('Negative sample mean distribution')
        # plt.show()


    def nnDataset(self, day_offset=0, shuffle='origin'):
        time = {0: ['2017-7-24', '2019-12-29', '2019-12-30', '2020-12-20'],
                1: ['2017-7-25', '2019-12-30', '2019-12-31', '2020-12-21'],
                2: ['2017-7-26', '2019-12-31', '2020-1-1', '2020-12-22'],
                3: ['2017-7-27', '2020-1-1', '2020-1-2', '2020-12-23'],
                4: ['2017-7-28', '2020-1-2', '2020-1-3', '2020-12-24'],
                5: ['2017-7-29', '2020-1-3', '2020-1-4', '2020-12-25'],
                6: ['2017-7-30', '2020-1-4', '2020-1-5', '2020-12-26']}

        dfs_week = []

        # training set
        dfs_training = [df.loc[time[day_offset][0]: time[day_offset][3]] for df in self.dfs]
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

            for n in range(len(dfs_training[i].index) // 672):
                week = dfs_training[i].iloc[n*672:n*672+672, :-1]
                week.index = range(len(week.index))
                dfs_week.append(week)
        week_set = pd.concat(dfs_week, axis=1)

        self.buildNegativeSamples_bymean(dfs_training, week_set, int(len(dfs_training[0].index) / 672 * 8 * 1))
        self.buildNegativeSamples_bypeak(dfs_training, week_set, int(len(dfs_training[0].index)/672 * 8 * 2))

        # unsupervised samples
        # for i in range(8*3):
        #     fake_group = pd.DataFrame(columns=range(8))
        #     randomlist = random.sample(range(0, 64), 8)
        #     print(randomlist)
        #     for c in range(8):
        #         # fake_group[c] = dfs_training[c].iloc[:, (i+c) % 8]
        #         fake_group[c] = dfs_training[randomlist[c] // 8].iloc[:, randomlist[c] % 8]
        #     # ==========shuffle columns=================
        #     if shuffle == 'random':
        #         fake_group = fake_group.sample(n=8, axis='columns')
        #         fake_group.columns = range(8)
        #     elif shuffle == 'sort':
        #         fake_group = fake_group.loc[:, fake_group.mean().sort_values(ascending=True).index]
        #         fake_group.columns = range(8)
        #     # ============================================
        #     fake_group['label'] = pd.Series(0, index=fake_group.index)
        #     dfs_training.append(fake_group)


        training_set = pd.concat(dfs_training)

        if shuffle == 'random':
            trainDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\classiferRandom'+str(day_offset)+'.csv'
        elif shuffle == 'sort':
            trainDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\classiferSorted' + str(
                day_offset) + '.csv'
        elif shuffle == 'origin':
            trainDataFile = 'C:\\Users\\yhu28\\Documents\\Code\\Research\\LoadGeneration\\dataset\\newRiver\\classiferOrigin' + str(
                day_offset) + '.csv'
        else:
            print('!!! unknown shuffle !!!')

        training_set.to_csv(trainDataFile)
        # testing_set.to_csv(testDataFile)

if __name__ == '__main__':
    dataloader = newRiverLoader()
    dataloader.load_data()
    # dataloader.nnDataset(0, shuffle='sort')
    for i in range(7):
        dataloader.nnDataset(i, shuffle='sort')
    # dataloader.groupDataset(day_offset=0)