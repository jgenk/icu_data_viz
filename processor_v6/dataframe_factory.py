# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 13:16:56 2016

@author: genkinjz
"""

import pandas as pd
import abc
from dataIDs import DataIDFactory
from dataIDs import DataConstants as dc
import mimic_processor as mimic
from random import sample


class DataFrameFactory(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self,db_name,data_def_filename,context_featurizer=None):
        self.dataID_fac = DataIDFactory(db_name,data_def_filename)
        self.processor = create_processor(db_name)
        self.__featurizers = []
        self.context_featurizer = context_featurizer

    def add_featurizer(self,featurizer):
        self.__featurizers.append(featurizer)
        return self.__featurizers
    
    def set_context_featurizer(self,featurizer):
        self.context_featurizer = featurizer
        return
        
    def create_ICU_DataFrame(self,adm_prop=1.0,random=False,duration=None):
        hadm_context_df = pd.DataFrame()
        
        index_tuples = []
        rows = []
        
        #Get Admission IDs
        hadm_ids = self.processor.all_hadm_ids()
        adm_cnt = int(round((len(hadm_ids)*adm_prop) + 0.5))
        if random: hadm_ids = sample(hadm_ids, adm_cnt)
        else: hadm_ids = hadm_ids[:adm_cnt]
        
        for hadm_id in set(hadm_ids):
            #get context information from this processor
            hadm_context = self.__get_context(hadm_id)
            hadm_context_df = hadm_context_df.append(hadm_context)
            #Get ICU information for this admission
            icu_info = self.__get_icu_info(hadm_id)
            admDT = hadm_context[self.dataID_fac.data_id_for_name("HOSPITAL_ADMISSION_DT").uniq_id] #uniq_id of Admission Datetime
            dcDT = hadm_context[self.dataID_fac.data_id_for_name("HOSPITAL_DISCHARGE_DT").uniq_id] #uniq_id of Discharge Datetime
            timeseries_dict={}
            
            #get required data for this hospital admission
            for dataID in self.__required_dataIDs():
                ts = self.processor.timeseries(hadm_id,dataID)
                ts_split =  ts.time_split(admDT,dcDT,duration) if not duration is None else [ts]
                for i in range(0,len(ts_split)):
                    ts_list=timeseries_dict.get(i,[])
                    ts_list.append(ts_split[i])
                    timeseries_dict[i] = ts_list
            
            #featurize the data
            for i,ts_list in timeseries_dict.iteritems():
                new_row = self.__to_feature_row(ts_list,icu_info,hadm_context)
                rows.append(new_row)
                index_tuples.append((hadm_id,i))

#        df = pd.concat(rows, axis=2, keys=[s.name for s in rows], ignore_index=True)
        df = pd.DataFrame(rows)
        df.index = pd.MultiIndex.from_tuples(index_tuples, names=['hadm_id', 'seq'])
        
        if self.context_featurizer is not None:
            context_df = self.context_featurizer.featurize(hadm_context_df)
#            df = pd.concat([df,context_df],axis=1)
            for hadm_id in df.index.get_level_values('hadm_id'):
                context_row = context_df.loc[hadm_id]
                for col in context_row.index:
                    df.loc[hadm_id,col] = context_row[col]
#                for i in .index:
#                    print df.loc[(hadm_id,i)]
#                    print context_row
#                    df.loc[(hadm_id,i)] = pd.concat([df.loc[(hadm_id,i)],context_row])
            
        return df
       
        
    def __to_feature_row(self, timeseries_list,icu_info,context_row):

        if len(timeseries_list) == 0: return None
        row = pd.Series()
        row.append(context_row)
        #iterate through featurizers and generate featurized data
        for featurizer in self.__featurizers:
            feature_data = featurizer.featurize(timeseries_list,icu_info=icu_info)
            if feature_data is None: continue
            row = row.append(feature_data)
        return row

    def __get_icu_info(self,hadm_id):
        icu_data_ids = self.dataID_fac.all_dataIDs(filter_on=dc.ICU_INFO)
        return self.processor.hadm_icu_info(hadm_id, icu_data_ids)
        
    def __get_context(self,hadm_id):
        context_data_ids = self.dataID_fac.all_dataIDs(filter_on=dc.HADM_CONTEXT)
        return self.processor.hadm_context(hadm_id,context_data_ids)
    
    def __required_dataIDs(self):
        return set(data_ID for featurizer in self.__featurizers for data_ID in featurizer.required_dataIDs())

def create_processor(db_name):
    if db_name == mimic.MIMIC: return mimic.MimicProcessor()
    return None
    

#def split_timeseries_list(tslist,admDT,dcDT,duration):
#    return map(list, zip(*[ts.time_split(admDT, dcDT, duration) for ts in tslist]))
