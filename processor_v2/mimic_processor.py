import icu_data_processor as IDP
import psql_utils as pu
from patient import Patient
import numpy as np
from fuzzywuzzy import fuzz


CHARTTIME = "charttime"
STARTTIME = "starttime"
VALUE = "value"
AMOUNT = "amount"
RATE = "rate"
TABLE_MAP = {"LABEVENTS" : [CHARTTIME,[VALUE]],
             "CHARTEVENTS" : [CHARTTIME,[VALUE]],
             "DATETIMEEVENTS" : [CHARTTIME, [VALUE]],
             "INPUTEVENTS_CV" : [CHARTTIME,[AMOUNT,RATE]],
             "INPUTEVENTS_MV" : [STARTTIME,[AMOUNT,RATE]],
             "OUTPUTEVENTS" : [CHARTTIME, [VALUE]],
             "PROCEDUREEVENTS_MV" : [STARTTIME,[VALUE]]}

#MICROBIOLOGYEVENTS not included due to complexity


LAB_DEF_TBL = "D_LABITEMS"
ALL_DEF_TBL = "D_ITEMS"

class MimicProcessor(IDP.ICUDataProcessor):

    def __init__(self,mimic_dbname='mimic',user='postgres',pw='123'):
        IDP.ICUDataProcessor.__init__(self)
        self.dbhelper = pu.DBHelper(mimic_dbname,user,pw, "mimiciii")


    def get_timeseries_for_varid(self,var_id,pt_id,start_dttime,end_dttime):
        timeseries = []
        #select all data that matches var_id and pt_id
        for table_name,table_specs in TABLE_MAP.iteritems():
            data = self.dbhelper.simple_select(table_name,
                                                    [table_specs[0]]+table_specs[1],
                                                    "itemid={0} AND subject_ID={1}".format(var_id,pt_id))
            if len(data) == 0: continue
            for row in data:
                timestamp = row[table_specs[0]]
                if (timestamp >= start_dttime) and (timestamp <= end_dttime):
                    values = [row[key] for key in table_specs[1]]
                    if verify_values(values): timeseries.append([timestamp]+values)

        return np.array(timeseries)

    def list_icustay_ids(self):
        return [icu_stay[0] for icu_stay in self.dbhelper.simple_select("ICUSTAYS", "icustay_id")]

    def get_icu_stay_info(self, icustay_id):
        """
        return ICUStay object
        """
        icustay_data = self.raw_data_for_ICUstay(icustay_id)
        pt_id = icustay_data[4]
        hadm_id = icustay_data[5]
        pt = get_pt_for_id(self.dbhelper, pt_id, hadm_id)

        outcome = IDP.ICUOutcome.TRANSFERRED
        if self.deceased_in_interval(icustay_data[2],icustay_data[3],hadm_id):
            outcome = IDP.ICUOutcome.DECEASED
        args = list(icustay_data[:4]) + [pt,outcome]
        return IDP.ICUStay(*args)

    def raw_data_for_ICUstay(self, icustay_id):
        """
        return [ID,unit_ID,start_dttime,end_dttime,pt_id,hadm_id]
        """
        icu_data = self.dbhelper.simple_select("ICUSTAYS",
                                                ["icustay_id","first_careunit","intime","outtime","subject_id","hadm_id"],
                                                "icustay_id='{0}'".format(icustay_id))[0]
        return icu_data

    def deceased_in_interval(self,start_dt,end_dt, hadm_id):
        if (start_dt is None) or (end_dt is None): return False
        death_time = self.dbhelper.simple_select("ADMISSIONS",["deathtime"],"hadm_id={0}".format(hadm_id))[0][0]
        if death_time is None: return False
        return (death_time > start_dt) and (death_time <= end_dt)

    def get_varid_for_name(self, name):
        return get_varid_for_name(self.dbhelper, name)

    def get_name_for_varid(self, var_id):
        varname = self.dbhelper.simple_select(LAB_DEF_TBL,"label","itemid={0}".format(var_id))
        if len(varname) == 0: varname=self.dbhelper.simple_select(ALL_DEF_TBL,"label","itemid={0}".format(var_id))
        if len(varname) == 0: return None
        return varname[0][0]

    def get_incustayIDs_for_criteria(self,var_id,mag=1,time_s=0):
        qualifying_ICUstays = []
        return


def get_pt_for_id(dbhelper, pt_id,hadm_id):
    pt_data =  dbhelper.simple_select("PATIENTS",["subject_id", "gender","dob"], "subject_id={0}".format(pt_id))[0]
    hadm_data = dbhelper.simple_select("ADMISSIONS","ethnicity","hadm_id={0}".format(hadm_id))[0]
    return Patient(*(pt_data + hadm_data))

def get_varid_for_name(dbhelper, name):

    options = get_varid_options_bylabel(dbhelper, name)
    if len(options) == 0:
        choice = None
    elif len(options) == 1:
        choice = 0
    else:
        choice = user_select_from_list(options)
    if choice == None : return None
    return options[choice][1]

def get_varid_options_bylabel(dbhelper, name,num_options=10):
    labels = dbhelper.simple_select(LAB_DEF_TBL,["label","itemid"])
    labels = labels + dbhelper.simple_select(ALL_DEF_TBL,["label","itemid"])
    max_score = 0
    for label_info in labels:
        score = fuzz.partial_ratio(name.upper(),label_info[0].upper())
        label_info.append(score)

    labels.sort(key=lambda label_info: 100-label_info[-1])

    return labels[:num_options]

def user_select_from_list(options,text_prompt="Which is correct? "):
    for i in range(0,len(options)): print i,options[i]
    index = raw_input(text_prompt).strip()
    if len(index) == 0 : return None
    return int(index)

def verify_values(vals):
    if vals is None: return False
    if len(vals) == 0: return False
    for val in vals:
        if len(val) == 0: return False
    return True
