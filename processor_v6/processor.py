# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:22:59 2016

@author: genkinjz
"""

import abc
from timeseries import Timeseries

class Processor(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        return
    
    def timeseries_bulk(self, hadm_ids, data_ids):
        timeseries_dict={}
        for hadm_id in hadm_ids:
            timeseries_list=[]
            for data_id in data_ids:
                timeseries_list.append(self.timeseries(hadm_id,data_id))
            timeseries_dict[hadm_id]=timeseries_list
        return timeseries_dict

    def timeseries(self, hadm_id, data_id):
        data = self.__timeseries_data(hadm_id,data_id)
        return Timeseries(hadm_id,data_id, data)
    
    @abc.abstractmethod
    def hadm_context(self,hadm_id,context_data_ids):
        return 
        
    @abc.abstractmethod
    def hadm_icu_info(self, hadm_id,icu_data_ids):
        return 

    @abc.abstractmethod
    def __timeseries_data(self, hadm_id,data_id):
        return 

    @abc.abstractmethod
    def all_hadm_ids(self,id_type=None, IDs=[]):
        return
    

