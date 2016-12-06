# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:02:36 2016

@author: genkinjz
"""

import pandas as pd



class Timeseries:
    def __init__(self, hadm_id, data_id, timeseries_data):
        self.hadm_id = hadm_id
        self.data_id = data_id
        self.series = timeseries_data.sort_index() if timeseries_data is not None else pd.Series()
        self.series = self.series.rename(self.data_id.uniq_id)
        
    def __str__(self):
        unformatted = "------\n{0}\n#Data Points:{1}\n------"
        return unformatted.format(self.data_id, self.series.size)

    def time_split(self,start_dt,end_dt,duration_td,hadm_id_index=True):
        split_list = []
        while start_dt < end_dt:
            next_dt = start_dt + duration_td
            split_ts = Timeseries(self.hadm_id,self.data_id,self.series[start_dt:next_dt])
            split_list.append(split_ts)
            start_dt = next_dt
        return split_list
        
