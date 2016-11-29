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
TABLE_MAP = {LABEVENTS : [CHARTTIME,[VALUE]],
             "CHARTEVENTS" : [CHARTTIME,[VALUE]],
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

        
        
    """
    @@@@@@@@@@@@@@@@@@@@@@@@@@@
    -----INHERITED METHODS-----
    @@@@@@@@@@@@@@@@@@@@@@@@@@@
    """

    def hadm_ids(self, id_type=None, IDs=[]):
        """
        Return a list of hadm_IDs.
        
        kwargs
            id_type=None : filter based on this id type
                None (default) : will retrieve timeseries from all hadm
                'icustay'
                'patient'
                'hadm'
            IDs=[] : list of ids, will only be used if a id_type is set
        
        """
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

        hadm_ids = [hadm[0] for hadm in self.dbhelper.simple_select("ICUSTAYS", "hadm_id",where,param)]
        return list(set(hadm_ids))
        
    
        
    def __timeseries_data(self,feature_id, hadm_id, startDT, duration):
        """
        Return pd series with indices as timestamps and data corresponding to
        a given feature_id, for a given hospital admission.
        
        """
        
        #get all item IDs corrresponding to this feature_id
        item_ids = self.__featureID_to_itemID(feature_id)
        #get map of all item_ids to their table
        item_map = self.__map_to_table(item_ids)
        #for each item_id, select all data that matches in a given icustay
        series_list = []
        for item_id,table_nm in item_map.iteritems():
            data = self.__get_data(table_nm, item_id, hadm_id, startDT, duration)
            series_list += data
        if len(series_list) == 0: return None
        df = pd.DataFrame(np.array(series_list))
        output_series = df.set_index(df[0], drop=True)[1]
        return output_series
        
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
     
        tb_patient,tb_hadm = "PATIENTS","ADMISSIONS"
        subject_id="subject_id"
        base_data = self.dbhelper.simple_select(tb_hadm, [subject_id],"hadm_id={0}".format(hadm_id))[0]
        context_df = pd.DataFrame([[hadm_id,base_data[subject_id]]])
        context_df.columns = ["hadm_id","pt_id"]

        #Set up mapping between context tags and data extraction from MIMIC_III
        table_info = {tb_patient : [subject_id,context_df.pt_id[0]],
                      tb_hadm : ["hadm_id",context_df.hadm_id[0]]}

        kw_df = pd.DataFrame([["pt_dob",tb_patient,"dob"],
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
            id_col = table_info[tb_nm][0]
            rec_id = table_info[tb_nm][1]
            data = self.dbhelper.simple_select(tb_nm,cols,'{0}={1}'.format(id_col, rec_id))[0]
            for i in temp_df.index.tolist():
                kw = temp_df.kw[i]
                context_df[kw] = data[temp_df.psql_col[i]]
        
     
        return context_df
        
    def get_icustay_info(self, hadm_id):
        """
        Returns a pd.DataFrame of information giving information on all ICU stays
        occuring within a given admission.
        
            icustay_id      icustay_inDT    icustay_outDT   icustay_icuname
        0
        1
        2
        ...

        """
        table = "ICUSTAYS"
        columns = ["icustay_id","intime","outtime","first_careunit"]
        where = "hadm_id={0}".format(hadm_id)

        df = pd.DataFrame(np.array(self.dbhelper.simple_select(table, columns, where)))
        df.columns = columns
        return df

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
            query = self.dbhelper.simple_select("D_ITEMS",["linksto"],"itemid={0}".format(item_id))
            if len(query) == 0: linksto = LABEVENTS
            else: linksto = query[0][0]
            item_map[item_id]=linksto.upper()
        return item_map
       
        
    def __get_data(self, table_nm, item_id, hadm_id, startDT, duration):
        """
        Gets all timeseries data from the passed table_nm for a given item_id
        and hadm_id. Will collect results from startDT through startDT+duration.
            table_nm = string
            item_id = int
            hadm_id = int
            startDT = datetime.datetime
            duration = datetime.timedelta
        """
        #extract information from args
        endDT = startDT + duration
        table_specs = TABLE_MAP[table_nm]
        time_col = table_specs[0]
        data_col = table_specs[1]
        
        #assemble psql query
        columns = [time_col]+data_col
        where = "itemid={0} AND hadm_id={1} AND {2}>=%(startDT)s AND {2}<=%(endDT)s".format(
                    item_id,hadm_id,time_col)
        param = {"startDT": startDT, "endDT" : endDT}
        
        return self.dbhelper.simple_select(LABEVENTS, columns, where,param)


    def __featureID_to_itemID(self, feature_id):
        id_dict = {}
        id_dict["LACTATE"] = [50813] #lactate
        id_dict["HEART_RATE"] = [211,220045]
        id_dict["HEMOGLOBIN"] = [1165,3759,51222,50811]
        id_dict["OXYGEN_SATURATION"] = [646,50817]

        #TODO - this needs to be fleshed out into a real mapping process
#        Use excel for csv managment and import with pd.DataFrames
    
        return id_dict[feature_id]
