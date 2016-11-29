# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 08:50:16 2016

@author: genkinjz

For testing:
    icustay with lactate = 216989
"""
import DBprocessor as dbp
import pandas as pd
import numpy as np
import psql_utils as pu

VALUE = "value"
AMOUNT = "amount"
RATE = "rate"
CHARTTIME = "charttime"
STARTTIME = "starttime"
LABEVENTS = "LABEVENTS"
TABLE_MAP = {"CHARTEVENTS" : [CHARTTIME,[VALUE]],
             "DATETIMEEVENTS" : [CHARTTIME, [VALUE]],
             "INPUTEVENTS_CV" : [CHARTTIME,[AMOUNT,RATE]],
             "INPUTEVENTS_MV" : [STARTTIME,[AMOUNT,RATE]],
             "OUTPUTEVENTS" : [CHARTTIME, [VALUE]],
             "PROCEDUREEVENTS_MV" : [STARTTIME,[VALUE]]}
        


class Processor(dbp.Processor):
    
    def __init__(self,mimic_dbname='mimic',user='postgres',pw='123'):
        dbp.Processor.__init__(self)
        self.dbhelper = pu.DBHelper(mimic_dbname,user,pw, "mimiciii")
        return
        
    def __timeseries_by_icustay(self,feature_id, icustay_id):
        """
        Return pd series with indices as timestamps and data corresponding to
        a given feature_id, for a given an ICU stay.
        """
        #get all item IDs corrresponding to this feature_id
        item_ids = self.__featureID_to_itemID(feature_id)
        #get map of all item_ids to their table
        item_map = self.__map_to_table(item_ids)
        #for each item_id, select all data that matches in a given icustay
        series_list = []
        for item_id,table_nm in item_map.iteritems():
            if table_nm == LABEVENTS: data = self.get_labdata(item_id,icustay_id)
            else: data = self.get_data(table_nm,item_id,icustay_id)
            series_list += data
        if len(series_list) == 0: return None
        df = pd.DataFrame(np.array(series_list))
        output_series = df.set_index(df[0], drop=True)[1]
        return output_series
        
    def __map_to_table(self,item_ids):
        item_map = {}
        for item_id in item_ids :
            query = self.dbhelper.simple_select("D_ITEMS",["linksto"],"itemid={0}".format(item_id))
            if len(query) == 0: linksto = LABEVENTS
            else: linksto = query[0][0]
            item_map[item_id]=linksto.upper()
        return item_map
    
    def get_labdata(self,item_id,icustay_id):
        icustay_info = self.get_context_info(icustay_id,icustay_inDT=1,
             icustay_outDT=1, hadm_id=1)
        inDT=icustay_info["icustay_inDT"][0]
        outDT=icustay_info["icustay_outDT"][0]
        hadm_ID=icustay_info["hadm_id"][0]
        
        columns = [CHARTTIME,VALUE]
        where = "itemid={0} AND hadm_id={1} AND {2}>%(inDT)s AND {2}<%(outDT)s".format(
                    item_id,hadm_ID,CHARTTIME)
        return self.dbhelper.simple_select(LABEVENTS, columns, where,{"inDT": inDT, "outDT" : outDT})
        
        
    def get_data(self,table_nm,item_id,icustay_id):
        
        table_specs = TABLE_MAP[table_nm]
        columns = [table_specs[0]]+table_specs[1]
        where = "itemid={0} AND icustay_id={1}".format(item_id,icustay_id)
        return self.dbhelper.simple_select(table_nm, columns, where)
    
    def __icustay_ids(self,**kwargs):
        """
        Return a list of icustayIDs.
        
        kwargs
            pt_id=None : will return only icustay_ids for a specific patient
            hadm_id=None : will return only icustay_ids for a specific hospital admission
        """
        #TODO: write this method
        where = []
        param = []
        if "pt_id" in kwargs.keys(): 
            where.append("subject_id=%s")
            param.append(kwargs["pt_id"])
        if "hadm_id" in kwargs.keys(): 
            where.append("hadm_id=%s")
            param.append(kwargs["hadm_id"])
        if len(where) > 0: where = " AND ".join(where)
        else: where=None
        
        
        return [icu_stay[0] for icu_stay in 
                      self.dbhelper.simple_select("ICUSTAYS", "icustay_id",where,param)]
        
        
    def __hadm_ids(self, pt_id=None):
        """
        Return a list of hadm_IDs.
        
        pt_id=None : will return only hadm_ids for a specific patient
        
        """
        #TODO: write this method
        return
        
    def __pt_ids(self):
        """
        Return a list of all patient IDs.
        
        """
        #TODO: write this method
        return

        
    def get_context_info(self, icustay_id, **kwargs):
        """
        Returns a dict of information giving context about an ICUSTAY based 
        on what is available in the database. Examples include patient demographics,
        medical history, ICU adm/dc time, ICU name, hadm information. Will always
        return icustay_id, hadm_id and pt_id
         
        kwargs: If any of these are set to 1, then only those set to 1 will be retrieved.
        if some are set to 0, but no others are set to 1, all except 0's will be set. 
            icustay_inDT
            icustay_outDT
            icustay_icuname
            pt_id
            pt_dob
            pt_age
            pt_ethnicity
            pt_gender
            pt_pmh
            hadm_id
            hadm_admDT
            hadm_dcDT
            hadm_deceased
        """
     
        tb_icustay,tb_patient,tb_hadm = "ICUSTAYS","PATIENTS","ADMISSIONS"
        base_data = self.dbhelper.simple_select(tb_icustay,
                                                ["subject_id","hadm_id"],
                                                "icustay_id='{0}'".format(icustay_id))[0]
        context_df = pd.DataFrame([[icustay_id,base_data["subject_id"],base_data["hadm_id"]]])
        context_df.columns = ["icustay_id","pt_id","hadm_id"]

        #Set up mapping between context tags and data extraction from MIMIC_III
        tb_info = {tb_icustay : ["icustay_id",icustay_id],
                   tb_patient : ["subject_id",context_df.pt_id[0]],
                   tb_hadm : ["hadm_id",context_df.hadm_id[0]]}

        kw_df = pd.DataFrame([["icustay_inDT",tb_icustay,"intime"],
                             ["icustay_outDT",tb_icustay,"outtime"],
                             ["icustay_icuname",tb_icustay,"first_careunit"],
                             ["pt_dob",tb_patient,"dob"],
                             ["pt_ethnicity",tb_hadm,"ethnicity"],
                             ["pt_gender", tb_patient,"gender"],
#                            ["pt_pmh",tb_patient,"",1],
                             ["hadm_admDT", tb_hadm,"admittime"],
                             ["hadm_dcDT", tb_hadm,"dischtime"],
                             ["hadm_deathDT", tb_hadm,"deathtime"]])
        kw_df.columns = ["kw","table","psql_col"]
        #set all possible values in kwargs dict based on input
        def_val = (len(kwargs) == 0)
        for kw in kw_df.kw : kwargs[kw] = kwargs.get(kw,def_val) 
        kw_df = kw_df[kw_df["kw"].isin([kw for kw in kwargs.keys() if kwargs[kw]])]

        for tb_nm in kw_df.table.unique():
            temp_df = kw_df[kw_df.table == tb_nm]
            cols = temp_df.psql_col.tolist()
            id_col = tb_info[tb_nm][0]
            rec_id = tb_info[tb_nm][1]
            data = self.dbhelper.simple_select(tb_nm,cols,'{0}={1}'.format(id_col, rec_id))[0]
            for i in temp_df.index.tolist():
                kw = temp_df.kw[i]
                context_df[kw] = data[temp_df.psql_col[i]]
        
     
        return context_df
         
    def __featureID_to_itemID(self, feature_id):
        id_dict = {}
        id_dict[0] = [50813] #lactate
        id_dict[1] = [211,220045]
        #TODO - this needs to be fleshed out into a real mapping process
#        Use excel for csv managment and import with pd.DataFrames
    
        return id_dict[feature_id]