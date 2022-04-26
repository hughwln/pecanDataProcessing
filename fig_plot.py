# -*- coding: utf-8 -*-
"""
Created on Fri Sep 24 16:19:51 2021

@author: lsong4
"""

import pickle
import pandas as pd
import matplotlib.pyplot as plt
from dateutil.parser import parse

date_start = parse('2015-08-18 00:00:00-05:00')
date_end = parse('2015-08-20 00:00:00-05:00')

reso = '15min'

fn = 'PecanStreet_Reconstructed/Data/' + '9982'

f = open(fn,'rb')

data = pickle.load(f)
grid = data[reso]['grid'][date_start:date_end]
air1 = data[reso]['air1'][date_start:date_end]
furnace1 = data[reso]['furnace1'][date_start:date_end]
refrigerator1 = data[reso]['refrigerator1'][date_start:date_end]
disposal1 = data[reso]['disposal1'][date_start:date_end]
dishwasher1 = data[reso]['dishwasher1'][date_start:date_end]
drye1 = data[reso]['drye1'][date_start:date_end]
microwave1 = data[reso]['microwave1'][date_start:date_end]

plt.figure(0, figsize=(12, 8))

plt.plot(grid.index, grid.values, label="grid") 
if 'solar' in data[reso].columns:
    solar = -data[reso]['solar'][date_start:date_end]
    plt.plot(solar.index, solar.values, label="solar")
if 'solar2' in data[reso].columns:
    solar2 = -data[reso]['solar2'][date_start:date_end]
    plt.plot(solar2.index, solar2.values, label="solar2")
plt.plot(air1.index, air1.values, label="air1")
plt.plot(furnace1.index, furnace1.values, label="furnace1")
plt.plot(refrigerator1.index, refrigerator1.values, label="refrigerator1")
plt.plot(disposal1.index, disposal1.values, label="disposal1")
plt.plot(dishwasher1.index, dishwasher1.values, label="dishwasher1")
plt.plot(drye1.index, drye1.values, label="drye1")
plt.plot(microwave1.index, microwave1.values, label="microwave1")

plt.legend()
