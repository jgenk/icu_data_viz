# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:22:59 2016

@author: genkinjz
"""

import abc
from timeseries import Timeseries

class Processor(object):
    """
    Abstract class, intended to be the layer of abstraction that takes any sata source
    and converts it into raw data that can be recognized, featurized, and using in an ML algorithmm
    
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        return
    
    def timeseries_bulk(self, hadm_ids, data_ids):
        """
        Returns a dictionary of lists of timeseries where the keys are the 
        hadm_ids passed in, and the values are all of the timeseries for the specifified
        data ids. Even if a given hadm has no data for a given data_id, an emplty timeseries
        will stil be returned.
        
        hadm_ids:
            List of hadm_ids
        data_ids: [dataIDs.DataID]
            List of data ides that will drive which data is pulled from database
        """
        timeseries_dict={}
        for hadm_id in hadm_ids:
            timeseries_list=[]
            for data_id in data_ids:
                timeseries_list.append(self.timeseries(hadm_id,data_id))
            timeseries_dict[hadm_id]=timeseries_list
        return timeseries_dict

    def timeseries(self, hadm_id, data_id):
        """
        Returns a single timeseries for a specific hospital admission, based on the
        data id
            hadm_id : int
            data_id : dataIDs.DataID
        """
        data = self.__timeseries_data(hadm_id,data_id)
        return Timeseries(hadm_id,data_id, data)
    
    @abc.abstractmethod
    def hadm_context(self,hadm_id,context_data_ids):
        """
        Returns context information about the hospital admission. 
        
            hadm_id : int
            context_data_ids : [dataIDs.DataID]
                List of dataIDs to define which context pieces are returned
        
        Return 
            pd.Series : columns defined by context_data_ids (DataID.uniq_id)
        """
        return 
        
    @abc.abstractmethod
    def hadm_icu_info(self, hadm_id,icu_data_ids):
        """
        Returns information about ICU stays during this hospital admission. 
        
            hadm_id : int
            icu_data_ids : [dataIDs.DataID]
                List of dataIDs to define which icu information pieces are returned
        
        Return 
            pd.DataFrame : columns defined by icu_data_ids (DataID.uniq_id), each row
                           is a separate ICU stay
        """
        return 

    @abc.abstractmethod
    def __timeseries_data(self, hadm_id,data_id):
        """
        Actuall retrieves a pd.Series object from the database; time indexed
        """
        return 

    @abc.abstractmethod
    def all_hadm_ids(self,id_type=None, IDs=[]):
        """
        Returns all hadm IDs from the database. Can be filtered for certain databases,
        i.e. based on a patient_id or based on an icustay_id.
        """
        return
    

