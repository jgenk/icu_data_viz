# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:28:31 2016

@author: genkinjz
"""
from featurizer import Featurizer
import pandas as pd
from dataIDs import DataConstants as dc


class ContextFeaturizer(Featurizer):

    def __init__(self,context_data_ids):
        Featurizer.__init__(self)
        self.data_ids = context_data_ids
        return   
        
    def can_featurize(self, data,**kwargs):
        return True,""
        
    
    def _Featurizer__do_featurize(self, context_df):
        output_df = pd.DataFrame()
        
        output_df = context_df.copy()
        
        for col in output_df.columns:
            if not self.__include(col): output_df.drop(col)
        for data_id in self.data_ids:
            if data_id.data_category == dc.NOMINAL:
                output_df = pd.get_dummies(output_df,columns=[data_id.uniq_id])
            elif data_id.data_category == dc.ORDINAL:
                output_df[data_id.uniq_id] = context_df[data_id.uniq_id].astype("category").cat.codes
        return output_df
    
        
    def __include(self,uniq_id):
        return uniq_id in [data_id.uniq_id for data_id in self.data_ids]
