# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 13:16:56 2016

@author: genkinjz
"""


import abc
import pandas as pd
import sys

ALL_ICU_STAYS='all_icustay'
SOME_ICU_STAYS='some_icustay'
PATIENT='patient'
HADM='hadm'

class Processor(object):
    __metaclass__ = abc.ABCMeta

    

    def __init__(self,default_granularity=ALL_ICU_STAYS):
        self.default_granularity = default_granularity
        return
	
    def timeseries(self, feature_id, granularity=None, **kwargs):
        """
        Return all timeseries objects for a given feature ID, based on a granularity.
        
        feature_id = int; ID of the feature that will be retrieved
        limit = max number of icustays to return
        kwargs
            limit=-1
            min_data=-1,
            granularity = dictates how the feature will be retrieved
                None (default) : will retrieve timeseries based on default
                'all_icustays'
                'some_icustays'
                'patient'
                'hadm'
            icustay_ids=[int] list of ICU stay ID. Must be set if granularity = SOME_ICU_STAYS
            patient_ids=[int] list of patient IDs. Must be set if granularity = PATIENT
            hadm_ids=[int] list of hadm IDs. Must be set if granularity = HADM
        """
        granularity = kwargs.get("granularity",self.default_granularity)
        limit = kwargs.get("limit",-1)
        min_data=kwargs.get("min_data",-1)
        
        icustay_ids=[]
        
        if granularity == ALL_ICU_STAYS: 
            icustay_ids = self.__icustay_ids() 
        elif granularity == SOME_ICU_STAYS:
            icustay_ids = kwargs['icustay_ids']
        elif granularity == PATIENT:
            for id in kwargs.get('patient_ids'): icustay_ids.append(self.__icustay_ids(pt_id=id))
        elif granularity == HADM:
            for id in kwargs.get('hadm_ids'): icustay_ids.append(self.__icustay_ids(hadm_id=id))
        else:
            #TODO: THROW ERROR; invalid granularity
            pass
        timeseries_list = []

        for icustay_id in icustay_ids:
            if len(timeseries_list) == limit: break
            series = self.__timeseries_by_icustay(feature_id, icustay_id)
            if series is None or series.size < min_data: continue
            context_info = self.get_context_info(icustay_id)
            timeseries_list.append(Timeseries(feature_id,series,context_info))
            sys.stdout.write('\r{0}/{1}'.format(len(timeseries_list),limit))
            
        return timeseries_list 
                                                
	
        
    @abc.abstractmethod
    def __timeseries_by_icustay(self,feature_id, icustay_id):
        """
        Return pd series with indices as timestamps and data corresponding to
        a given feature_id, for a given an ICU stay.
        """
              
        return
        
        
    @abc.abstractmethod
    def __icustay_ids(self,**kwargs):
        """
        Return a list of icustayIDs.
        
        kwargs
            pt_id [None] : will return only icustay_ids for a specific patient
            hadm_id [None] : will return only icustay_ids for a specific hospital admission
        """
        return
        
    @abc.abstractmethod
    def __hadm_ids(self, pt_id=None):
        """
        Return a list of hadm_IDs.
        
        pt_id=None : will return only hadm_ids for a specific patient
        
        """
        return
        
    @abc.abstractmethod
    def __pt_ids(self):
        """
        Return a list of all patient IDs.
        
        """
        return

        
    @abc.abstractmethod
    def get_context_info(self, icustay_id, **kwargs):
         """
         Returns a dataframe of information giving context about a time series based 
         on what is available in the database. Examples include patient demographics,
         medical history, ICU adm/dc time, ICU name, hadm information.
         
         kwargs: If any of these are set to 1, then only those set to 1 will be retrieved
             icustay_inDT
             icustay_outDT
             icustay_deceased
             icustay_icuname
             pt_dob
             pt_age
             pt_ethnicity
             pt_gender
             pt_pmh
             hadm_admDT
             hadm_dcDT
             hadm_deceased
         """
         return 
		
class Timeseries:
    def __init__(self, feature_id, timeseries_data, context_df):
        self.feature_id = feature_id
        self.series = pd.to_numeric(timeseries_data.sort_index())
        self.context_df=context_df
    
    def __str__(self):
        return "------\n{0}\n#Data Points:{1}\n\n-Context-\n{2}\n------".format(feature_name_for_id(self.feature_id),
                                        self.series.size,
                                        self.context_df.transpose())
        
    #TODO: finish this class (convenience functions)

    


def feature_name_for_id(fid):
    dict_fname = {}
    dict_fname[0] = "Lactate"
    dict_fname[1] = "Heart Rate"
    #TODO: export this method to csv + pd.df

    return dict_fname[fid]
