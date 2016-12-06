# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 13:16:56 2016

@author: genkinjz
"""

import pandas as pd

FEATURE_DICT={"LACTATE":"LA",
              "HEART RATE":"HR",
              "HEMOGLOBIN":"HGB",
              "OXYGEN SATURATION":"SPO2"
              }

class Feature:
    def __init__(self, hadm_id, feature_id, timeseries_data, icuinfo_df):
        self.hadm_id = hadm_id
        self.feature_id = feature_id
        self.series = timeseries_data.sort_index() if timeseries_data is not None else pd.Series()
        self.series = self.series.rename(self.feature_id.name)
        self.icuinfo_df=icuinfo_df
        
    def __str__(self):
        unformatted = "------\n{0}\n#Data Points:{1}\n\n-Context-\n{2}\n\n-ICU Info-\n{3}\n------"
        return unformatted.format(self.feature_id,
                                    self.series.size,
                                    self.icuinfo_df)


    
class FeatureID(object):
    def __init__(self,name,abbrv):
        self.name = name
        self.abbrv = abbrv
    
    #TODO: set up comparable 

    
def make_featureID(name):
    abbrv = FEATURE_DICT[name]
    return FeatureID(name,abbrv)
    
def all_features():
    
    #TODO: export this method to csv + pd.df

    return [make_featureID(name) for name in FEATURE_DICT.keys()]
