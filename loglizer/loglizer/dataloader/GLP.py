"""
The interface to load GLP log datasets.

Authors:
    Hans Aschenloher
"""

import random
import pandas as pd
import os
import numpy as np
import re
from sklearn.utils import shuffle
from collections import OrderedDict
from datetime import datetime, timedelta

DATEFORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

def sessionWindow(df):
    data_dict = OrderedDict()
    return data_dict

class GlpDataloder():
    def __init__(self, online=False):
        self.online = online
        self.buffer = pd.DataFrame()


    def loadDataset(self, log_file, window='sliding', time_interval=60, stepping_size=30,
                train_ratio=0.7):

        # Load the file and sort lines according to time.
        df = pd.read_csv(log_file,low_memory=False)
        df = df.sort_values(by="Timestamp")
        df.reset_index(drop=True, inplace=True)
        df['LineId'] = range(0, df.shape[0])

        if window == 'fixed' or window == 'sliding':
            examples = self.slidingWindow(df,time_interval,stepping_size,window)
        elif window == 'session':
            examples = self.sessionWindow(df)

        random.shuffle(examples)
        #x = [[t[0],t[2]] for t in examples]
        x = [t[0] for t in examples]
        y = [t[1] for t in examples]
        i = [t[2] for t in examples]
        t0 = [t[3] for t in examples]
        t1 = [t[4] for t in examples]

        n_train = int(len(x) * train_ratio)

        x_train = np.array(x[:n_train], dtype=list)
        i_train = np.array(i[:n_train], dtype=list)
        y_train = np.array(y[:n_train], dtype=int)
        x_test  = np.array(x[n_train:], dtype=list)
        y_test  = np.array(y[n_train:], dtype=int)


        t0_train = np.array(t0[:n_train], dtype=datetime)
        t1_train = np.array(t1[:n_train], dtype=datetime)

        print('Total: {} instances, {} anomaly, {} normal' \
            .format(len(y), sum(y), len(y) - sum(y)))
        print('Train: {} instances, {} anomaly, {} normal' \
            .format(len(y_train), sum(y_train), len(y_train) - sum(y_train)))
        print('Test: {} instances, {} anomaly, {} normal' \
            .format(len(y_test), sum(y_test), len(y_test) - sum(y_test)))

        return (x_train, y_train, i_train, t0, t1), (x_test, y_test)


    def slidingWindow(self, df,time_interval, stepping_size, window, timestamp='Timestamp'):

        windows = [] # List of sequences and anomaly labels.

        if self.online:
            df = pd.concat([self.buffer,df], ignore_index=True)


        start_time = df[timestamp][0]
        end_time = df[timestamp].iloc[-1]
        start_time = datetime.strptime(start_time, DATEFORMAT)
        end_time = datetime.strptime(end_time, DATEFORMAT)
        time_interval = timedelta(seconds=time_interval)
        stepping_size = timedelta(seconds=stepping_size)

        index = 0
        t0 = start_time
        t1 = t0 + time_interval
        while t1 < end_time:
            sequence = []
            is_anomaly = 0
            # Make a sequence and label it as normal or abnormal.
            while datetime.strptime(str(df[timestamp][index]), DATEFORMAT) < t1:
                sequence.append(df['EventId'][index])
                if 'Label' in df.columns:
                    if df['Label'][index] == '-':
                        is_anomaly = 1
                index += 1
            if sequence:
                if 'index' in df.columns:
                    windows.append([sequence, is_anomaly, df['LineId'][index], t0, t1, df['index'], df['id']])
                else:
                    windows.append([sequence, is_anomaly, df['LineId'][index], t0, t1])
            # Translate the window.
            if window == "fixed":
                t0 = t1
            elif window == "sliding":
                t0 += stepping_size
            t1 = t0 + time_interval

        if self.online:
            if len(windows) > 0:
                self.buffer = df[df['LineId'] >= windows[-1][2]]
            else:
                self.buffer = df
        return windows
