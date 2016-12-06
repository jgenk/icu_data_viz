# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 13:16:56 2016

@author: genkinjz
"""

import pandas as pd

class DataFrameFactory(object):
    def __init__(self,proc,featurizer_set):
        self.processor = proc
        self.featurizers = featurizer_set
        self.duration = None    
    
    def set_duration(self,timedelta):
        self.duration=timedelta
        return
    
    def context_df(self,hadm_ids,**kwargs):
        context_df = pd.DataFrame
        for hadm_id in set(hadm_ids):
            context_info = self.processor.get_context_info(hadm_id,**kwargs)
            context_df.append(context_info)
        
               
        return self.featurize_context(context_df)
        
    def create_feature_df(self, hadm_ids,feature_list):
        df = pd.DataFrame()
        df.columns = self.get_column_names()
        for hadm_id in set(hadm_ids):
            row = self.create_feature_row(hadm_id,feature_list)
            df = df.append(row)
        return df
    
    def create_feature_row(self, hadm_id, feature_list):
        feature_list = []
        for feature_id in feature_list:
            feature = get_feature(hadm_id, feature_id)
            raw_feature = self.processor.raw_feature_by_hadm_id(feature_id, 
                                                                  hadm_id,
                                                                  duration=self.duration)
            feature_list.append(raw_feature)

        row = row_for_features(proc, hadm_id, feature_list)
    
    def get_column_names(self):
        return [col_name for featurizer in self.featurizers for col_name in featurizer.get_column_names()]