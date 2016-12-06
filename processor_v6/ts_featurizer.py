# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:28:31 2016

@author: genkinjz
"""
from featurizer import Featurizer
import abc
import pandas as pd

class TSFeaturizer(Featurizer):
    
    def __init__(self):
        Featurizer.__init__(self)
        return

    
    def can_featurize(self, ts_list,**kwargs):
        if type(ts_list) is not list: return False,"Must pass a list of timeseries"
        ts_dataIDs = set([ts.data_id for ts in ts_list])
        return self.required_dataIDs().issubset(ts_dataIDs),"Not all required data is included in this input"
        
        
    @abc.abstractmethod
    def required_dataIDs(self):
        return    

        
class BasicTSFeaturizer(TSFeaturizer):

    def __init__(self,data_id):
        TSFeaturizer.__init__(self)
        self.data_id = data_id
        return
        
    def _Featurizer__do_featurize(self, ts_list,icu_info=None,**kwargs):
        """
        Returns a series for this feature with column names (series indices) assigned appropriately
        """

        ts_row = pd.Series()
        for ts in ts_list:
            if ts.data_id != self.data_id: continue
            ts_row = ts_row.append(self.__convert_to_row(ts))
            break
        return ts_row
          
    def __convert_to_row(self,ts):
        row = pd.to_numeric(ts.series,errors='ignore').describe()
        row.index = ['{}_{}'.format(ts.data_id.uniq_id,index) for index in row.index]
        return row
        
    def required_dataIDs(self):
        return set([self.data_id])    
    

    
    



