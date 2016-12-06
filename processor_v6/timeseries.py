# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:02:36 2016

@author: genkinjz
"""

import pandas as pd



class Timeseries(object):
    """
    Wrapper for timeseries data, contains a time-indexed pandas.Series object as
    well as a dataIDs.DataID object describing what the data is and the hadm_id
    signifying where the data came from.
    """
    def __init__(self, hadm_id, data_id, timeseries_data):
        """
        hadm_id = int, unique ID for the hospital visit
        data_id = a dataIDs.DataID object describing data held in this Timeseries
        timeseries_data = pandas.Series object, timeindexed
        """
        self.hadm_id = hadm_id
        self.data_id = data_id
        self.series = timeseries_data.sort_index() if timeseries_data is not None else pd.Series()
        self.series = self.series.rename(self.data_id.uniq_id)
        
    def __str__(self):
        unformatted = "------\n{0}\n#Data Points:{1}\n------"
        return unformatted.format(self.data_id, self.series.size)

    def time_split(self,start_dt,end_dt,duration_td,hadm_id_index=True):
        """
        Splits the timeseries based on two endpoints, divided by a given duration.
        For example, if start_dt = 0, end_dt = 3, duration_td = 1, then this will
        produce 3 sub timeseries from 0-1, 1-2, 2-3.
        """
        split_list = []
        while start_dt < end_dt:
            next_dt = start_dt + duration_td
            split_ts = Timeseries(self.hadm_id,self.data_id,self.series[start_dt:next_dt])
            split_list.append(split_ts)
            start_dt = next_dt
        return split_list
        
