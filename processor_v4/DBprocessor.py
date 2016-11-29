# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 13:16:56 2016

@author: genkinjz
"""


import abc
import pandas as pd
import sys
from random import shuffle

class Processor(object):
    __metaclass__ = abc.ABCMeta

    

    def __init__(self):
        return
    

    
    def bulk_timeseries(self, feature_id, **kwargs):
        """
        Return all timeseries objects for a given feature ID, based on a granularity.
        
        feature_id = int; ID of the feature that will be retrieved
        limit = max number of icustays to return
        kwargs
            ts_max=-1 : max number of timeseries to return
            data_limits=(-1,-1) : (min, max) tuple, amount of data to have in each timeseries
            duration_max=None : max duration for time for timeseries
            id_type=None : filter based on this id type. see DBprocessor.hadm_ids for details
            IDs=[] : list of ids, will only be used if a id_type is set
            randomize=False : if True, hadmid processed will proceed in a random order
        """
        #set up filter and limits
        ts_max = kwargs.get("ts_max",-1)

        duration_max = kwargs.get("duration_max",None)
        randomize = kwargs.get("randomize",False)
        id_type = kwargs.get("id_type",None)
        id_list = kwargs.get("IDs",[])
        
        hadm_ids = self.hadm_ids(id_type,id_list)
        if randomize : shuffle(hadm_ids)

        timeseries_list = []

        for hadm_id in hadm_ids:
            #check to see if we have retrieved our limit
            if len(timeseries_list) == ts_max: break
            
            #Create timeseries based on feature_if and hadm_id
            ts = self.timeseries_by_hadm_id(feature_id, hadm_id,duration=duration_max)
            
            #check data limits
            min_data,max_data=kwargs.get("data_limits",(-1,None))
            size = ts.series.size
            if max_data is None: max_data = size
            if (ts is None) or (size < min_data) or (size > max_data) : continue
            
            #add to timeseries list
            timeseries_list.append(ts)
            sys.stdout.write('\r{0}/{1}'.format(len(timeseries_list),ts_max))
            
        return timeseries_list 
    
    def timeseries_by_hadm_id(self, feature_id, hadm_id, startDT=None, duration=None):
        """
        Method to bundle creation of a Timeseries object. Can be called publicly. This
        will create a single timeseries whereas bulk_timeseries will create a list
        of timeseries based on parameters.
        
        Args
            feature_id
            hadm_id
            startDT=None
            duration=None
        
        Returns
            List of timeseries objects
        """
        context_df = self.get_context_info(hadm_id)
        icuinfo_df = self.get_icustay_info(hadm_id)
        if startDT is None: startDT = context_df.hadm_admDT[0]
        if duration is None: duration = context_df.hadm_dcDT[0]-startDT
        timeseries_data = self.__timeseries_data(feature_id, hadm_id, startDT, duration)
        
        return Timeseries(feature_id, timeseries_data, context_df, icuinfo_df)
    
    @abc.abstractmethod
    def hadm_ids(self, id_type=None, IDs=[]):
        """
        Return a list of hadm_IDs.
        
        kwargs
            id_type=None : filter based on this id type
                None (default) : will retrieve timeseries from all hadm
                'icustay'
                'patient'
                'hadm'
            ids=[] : list of ids, will only be used if a id_type is set
        
        """
        return
    
        
    @abc.abstractmethod
    def __timeseries_data(self,feature_id, hadm_id, startDT, duration):
        """
        Return pd series with indices as timestamps and data corresponding to
        a given feature_id, for a given hospital admission.
        
        """
              
        return
        
    @abc.abstractmethod
    def get_context_info(self, hadm_id, **kwargs):
        """
        Returns a dataframe of information giving context about an HOSPITAL ADMISSION based 
        on what is available in the database. Examples include patient demographics,
        hadm information. Will always return hadm_id and pt_id
         
        kwargs: If any of these are set to 1, then only those set to 1 will be retrieved
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
   
    @abc.abstractmethod
    def get_icustay_info(self, hadm_id, **kwargs):
        """
        Returns a pd.DataFrame of information giving information on all ICU stays
        occuring within a given admission.
        
            icustay_id      icustay_inDT    icustay_outDT   icustay_icuname
        0
        1
        2
        ...
         
        kwargs: If any of these are set to 1, then only those set to 1 will be retrieved
             inDT=0 : transfer in datetime
             outDT=0 : transfer out datetime
             unit_name=0 : unit name
        """
        return 
                
		
class Timeseries:
    def __init__(self, feature_id, timeseries_data, context_df, icuinfo_df):
        self.feature_id = feature_id
        self.series = timeseries_data.sort_index() if timeseries_data is not None else pd.Series()
        self.series = self.series.rename(self.feature_id)
        self.context_df=context_df
        self.icuinfo_df=icuinfo_df
        
        
    def pt_id(self):
        return self.context_df.pt_id[0]
        
    def __str__(self):
        unformatted = "------\n{0}\n#Data Points:{1}\n\n-Context-\n{2}\n\n-ICU Info-\n{3}\n------"
        return unformatted.format(self.feature_id,
                                    self.series.size,
                                    self.context_df.transpose(),
                                    self.icuinfo_df)
                
    #TODO: finish this class (convenience functions)

    


def feature_name_for_id(fid):
    dict_fname = {}
    dict_fname[0] = "LACTATE"
    dict_fname[1] = "HEART RATE"
    dict_fname[2] = "HEMOGLOBIN"
    
    
    #TODO: export this method to csv + pd.df

    return dict_fname[fid]
