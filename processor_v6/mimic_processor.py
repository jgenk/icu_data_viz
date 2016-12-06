# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 16:22:59 2016

@author: genkinjz
"""

import abc
from processor import Processor
import psql_utils as pu
import pandas as pd
import numpy as np

MIMIC = 'mimic'
VALUE = "value"
AMOUNT = "amount"
RATE = "rate"
CHARTTIME = "charttime"
STARTTIME = "starttime"
LABEVENTS = "LABEVENTS"
TABLE_MAP = {LABEVENTS : [CHARTTIME,[VALUE]],
             "CHARTEVENTS" : [CHARTTIME,[VALUE]],
             "DATETIMEEVENTS" : [CHARTTIME, [VALUE]],
             "INPUTEVENTS_CV" : [CHARTTIME,[AMOUNT,RATE]],
             "INPUTEVENTS_MV" : [STARTTIME,[AMOUNT,RATE]],
             "OUTPUTEVENTS" : [CHARTTIME, [VALUE]],
             "PROCEDUREEVENTS_MV" : [STARTTIME,[VALUE]]}

class MimicProcessor(Processor):
    __metaclass__ = abc.ABCMeta

    def __init__(self,mimic_dbname=MIMIC,user='postgres',pw='123'):
        Processor.__init__(self)
        self.dbh = pu.DBHelper(mimic_dbname,user,pw, "mimiciii")
        return
        
    """
    @@@@@@@@@@@@@@@@@@@@@@@@@@@
    -----INHERITED METHODS-----
    @@@@@@@@@@@@@@@@@@@@@@@@@@@
    """

    def hadm_context(self,hadm_id,context_data_ids):

        #SETUP
        subject_id,hadm_id_str,tb_adm="subject_id","hadm_id","ADMISSIONS"
        base_data = self.dbh.simple_select(tb_adm, [subject_id],"{0}={1}".format(hadm_id_str,hadm_id))[0]
        pt_id = base_data[subject_id]
        data = []
        index = []
        data_map = {"PATIENTS" : (subject_id,pt_id,[]), 
                       tb_adm : (hadm_id_str,hadm_id,[])}

        #DETERMINE WHICH DATA WILL BE RETRIEVED
        for data_id in context_data_ids:
            db_specs=data_id.map_info.split(",")
            tb_nm = db_specs[0]
            tb_col = db_specs[1]
            data_map[tb_nm][2].append((tb_col,data_id.uniq_id))
            
        #RETRIEVE CONTEXT DATA
        for db_nm,query_info in data_map.iteritems():
            id_col = query_info[0]
            id_val = query_info[1]
            col_tuples = query_info[2] #list of tuples (col_nm, data_id.uniq_id)
            cols = [c[0] for c in col_tuples] 
            if len(cols) == 0: continue
            results = self.dbh.simple_select(db_nm,cols,"{0}={1}".format(id_col,id_val))[0]
            for tup in col_tuples: 
                data.append(results[tup[0]])
                index.append(tup[1])
                
        context_series = pd.Series(data)
        context_series.index = index
        context_series.name = hadm_id
        context_series.sort_index(inplace=True)
        context_series.fillna(value=np.nan,inplace=True)
        return context_series
        
    def hadm_icu_info(self, hadm_id,icu_data_ids):
        table = "ICUSTAYS"
        psql_columns = [data_id.map_info for data_id in icu_data_ids]
        where = "hadm_id={0}".format(hadm_id)

        df = pd.DataFrame(np.array(self.dbh.simple_select(table, psql_columns, where)))
        df.columns = [data_id.uniq_id for data_id in icu_data_ids]
        return df

    def all_hadm_ids(self, id_type=None, IDs=[]):
        #set default values for query args
        where = None
        param = []
        
        
        #If an id_type is passed in, we need to filter results based on that
        if id_type is not None: 
            #map id_type to actual column in table
            type_dict = {"icustay" : "icustay_id", "patient": "subject_id", "hadm" : "hadm_id"}
            #create where x IN tuple statement
            where = "{0} IN %s".format(type_dict[id_type])
            param = [tuple(IDs)]

        hadm_ids = [hadm[0] for hadm in self.dbh.simple_select("ICUSTAYS", "hadm_id",where,param)]
        return list(set(hadm_ids))

    def _Processor__timeseries_data(self,hadm_id,data_id):
        #get all item IDs corrresponding to this feature_id
        item_ids = data_id.map_info.split(",")
        #get map of all item_ids to their table
        item_map = self.__map_to_table(item_ids)
        #for each item_id, select all data that matches in a given icustay
        series_list = []
        for item_id,table_nm in item_map.iteritems():
            data = self.__get_data(table_nm, item_id, hadm_id)
            series_list += data
        if len(series_list) == 0: return None
        df = pd.DataFrame(np.array(series_list))
        output_series = df.set_index(df[0], drop=True)[1]
        return output_series
    
    """
    @@@@@@@@@@@@@@@@@@@@@@@@
    -----HELPER METHODS-----
    @@@@@@@@@@@@@@@@@@@@@@@@
    """
  
    def __map_to_table(self,item_ids):
        """
        Maps item ids to the tables in which they are included. 
        Uses the "LINKSTO" column of the D_ITEMS table.
        """
        item_map = {}
        for item_id in item_ids :
            query = self.dbh.simple_select("D_ITEMS",["linksto"],"itemid={0}".format(item_id))
            if len(query) == 0: linksto = LABEVENTS
            else: linksto = query[0][0]
            item_map[item_id]=linksto.upper()
        return item_map
       
        
    def __get_data(self, table_nm, item_id, hadm_id):
        """
        Gets all timeseries data from the passed table_nm for a given item_id
        and hadm_id.
        """

        #extract information from args
        table_specs = TABLE_MAP[table_nm]
        time_col = table_specs[0]
        data_col = table_specs[1]
        
        #assemble psql query
        columns = [time_col]+data_col
        where = "itemid={0} AND hadm_id={1}".format(item_id,hadm_id)        

        return self.dbh.simple_select(table_nm, columns, where)

