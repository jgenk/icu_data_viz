# -*- coding: utf-8 -*-
"""
Created on Tue Nov 29 13:40:54 2016

@author: genkinjz
"""

import DBPmimic as dbp
import pandas as pd
from random import sample

def main():
    
    proc = dbp.Processor()
    hadm_ids = get_hadm_ids(proc,20)
    feature_list = ["LACTATE","HEMOGLOBIN","OXYGEN_SATURATION"]
    for hadm_id in hadm_ids:
        df = create_df_for_features(proc, hadm_id, feature_list)
    print df
#    proc.timeseries_by_hadm_id(0, hadm_id)
#    
#    proc.get_context_info(190659)
#    proc.hadm_ids(id_type="icustay",IDs=[216989])
#    print proc.timeseries_by_hadm_id(1,139061)
#    
#    lactate_ts_list = proc.bulk_timeseries(0,randomize=True,data_limits=(10,None),ts_max=1) #Lactate
#    pt_ids = [ts.context_df.PT_ID[0] for ts in lactate_ts_list]
#    hr_ts_list = proc.bulk_timeseries(1,id_type='patient',IDs=pt_ids) #Lactate
#    
#    lactate_dict = {}
#    for ts in lactate_ts_list: lactate_dict[ts.pt_id]=ts
#    HR_dict = {}
#    for ts in hr_ts_list: HR_dict[ts.pt_id]=ts


def create_df_for_features(processor, hadm_id, feature_ids):
    feat_dict = features_for_hadm_id(processor, hadm_id, feature_ids)
    df = pd.DataFrame()
    for key in feat_dict.keys():
        ts = feat_dict[key]
        ts_describe = pd.to_numeric(ts.series).describe()
        print ts.series
        print ts_describe
    return df

def plot_ts(ts):
    series = shift_ts_to_DT(ts, ts.context_df.hadm_admDT[0])
    series = pd.to_numeric(series)
    series.plot.line()
    return
    
def features_for_hadm_id(proc, hadm_id, feature_ids):
    out_dict = {}
    for f_id in feature_ids:
        out_dict[f_id]= proc.timeseries_by_hadm_id(f_id, hadm_id)
    return out_dict
    
def shift_ts_to_DT(ts, startDT):
    new_index = (ts.series.index-startDT).total_seconds()
    new_series = ts.series.copy()
    new_series.index = new_index
    return new_series

def get_hadm_ids(proc,cnt):
    return sample(proc.hadm_ids(), cnt)
    
    
if __name__ == "__main__":
    main()