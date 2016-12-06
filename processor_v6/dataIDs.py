# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:52:33 2016

@author: genkinjz
"""
import pandas as pd
from functools import total_ordering

ID = "ID"
NAME = "NAME"
ABBRV = "ABBRV"
DATA_CAT = "DATA_CAT"
FILTER_TAG = "FILTER_TAG"

@total_ordering
class DataID(object):
    def __init__(self,row,map_col):
        self.uniq_id = row.name
        self.name = row[NAME]
        self.abbrv = row[ABBRV]
        self.data_category = row[DATA_CAT]
        self.tag = row[FILTER_TAG]
        self.map_info = row[map_col]
    
    def __str__(self):
        return "#{0}, {1} ({2}) - {3}, {4}\n\t{5}".format(self.uniq_id,
                                                          self.name,
                                                          self.abbrv,
                                                          self.data_category,
                                                          self.tag,
                                                          self.map_info)
    
    def __eq__(self, other):
        return self.uniq_id == other.uniq_id

    def __lt__(self, other):
        return self.uniq_id < other.uniq_id


class DataIDFactory(object):
    def __init__(self,database,filename):
        self.dataID_df = get_dataID_DataFrame(filename)
        self.database = database
        
    
    def data_id_for_name(self,name):
        return self.__makeID(self.dataID_df[self.dataID_df.NAME == name].iloc[0])

    def data_id(self,uniq_id):
        return self.__makeID(self.dataID_df[self.dataID_df.ID == uniq_id].iloc[0])
    
    def all_dataIDs(self,filter_on=None):
        return [self.__makeID(row[1]) for row in self.dataID_df.iterrows() 
                                        if (filter_on is None) or
                                           (filter_on == row[1][FILTER_TAG])]
    def __makeID(self,row):
        return DataID(row,self.database)

    
class DataConstants(object):
    NOMINAL = "NOMINAL"
    ORDINAL = "ORDINAL"
    CONTINUOUS = "CONTINUOUS"
    ICU_INFO = "ICU_INFO"
    HADM_CONTEXT = "HADM_CONTEXT"
    TIMESERIES = "TIMESERIES"

    
    
def get_dataID_DataFrame(filename):
    df = pd.read_csv(filename)
    df = df.set_index(ID)
    return df
 