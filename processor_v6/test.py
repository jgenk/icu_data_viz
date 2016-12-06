# -*- coding: utf-8 -*-
"""
Created on Mon Dec 05 13:19:45 2016

@author: genkinjz
"""
from ts_featurizer import BasicTSFeaturizer
from timeseries import Timeseries
from dataIDs import DataIDFactory
from dataIDs import DataConstants as dc
import pandas as pd
import numpy as np
import mimic_processor as mp
from dataframe_factory import DataFrameFactory
from context_featurizer import ContextFeaturizer
import time

MIMIC="mimic"

def main():
#    data_id_factory_test()
#    processor_test(5)
    basic_test()
    pass


def data_id_factory_test():
    factory = DataIDFactory("mimic","datadef.csv")
    print "1. All Data IDs"
    for i in factory.all_dataIDs(): print i

    print "\n2. Data ID for Name, 'LACTATE'"
    print factory.data_id_for_name("LACTATE")
    
    print "\n3. Filter on Hospital Admission Context"
    for i in factory.all_dataIDs(filter_on=dc.HADM_CONTEXT): print i
    pass 

    
def processor_test(hadm_cnt):
    factory = DataIDFactory("mimic","datadef.csv")
    processor = mp.MimicProcessor()
    #TODO: Add randomization?
    hadm_ids = processor.all_hadm_ids()
    print len(hadm_ids)
    hadm_ids = hadm_ids[:hadm_cnt]
    data_ids = factory.all_dataIDs(filter_on=dc.TIMESERIES)
    ts_list=processor.timeseries_bulk(hadm_ids,data_ids)
    for ts in ts_list:print ts
    pass        

def basic_test(db=MIMIC,adm_prop=1.0/5000,random=False,fn_extra=""):

    #Initialize DataFrame factory
    df_factory = DataFrameFactory(mp.MIMIC,"datadef.csv")
    dataID_fac = df_factory.dataID_fac
    
    #Assemble timeseries featurizers & add to dff
    data_ids = dataID_fac.all_dataIDs(filter_on=dc.TIMESERIES)
    for data_id in data_ids: 
        df_factory.add_featurizer(BasicTSFeaturizer(data_id))
    
    #Create context featurizer: This defines both which pieces of context will be
    #included, and how those pieces of context will be featurized
    data_ids = dataID_fac.all_dataIDs(filter_on=dc.HADM_CONTEXT)
    context_featurizer = ContextFeaturizer(data_ids)
    df_factory.context_featurizer = context_featurizer
    
    #Create dataframe!
    df = df_factory.create_ICU_DataFrame(adm_prop,random)
    print df

    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = "{}_{}_{}_{}".format(timestr,
                                 db,
                                 str(adm_prop),
                                 str(random),
                                 "_" + fn_extra if len(fn_extra) > 0 else "")

    df.to_csv("output/{}.csv".format(filename))
    return
    
def featurizer_test():
    print "Create timeseries data"
    rng = pd.date_range('1/1/2011', periods=72, freq='H')
    ts_data = pd.Series(np.random.randn(len(rng)), index=rng)
    print "Data:", ts_data
    
    print "\nCreate timeseries object"
    hadm_id = 1234
    factory = DataIDFactory()
    all_data_ids = factory.all_dataIDs()

    ts_list = [Timeseries(hadm_id,data_ID,ts_data) for data_ID in all_data_ids]
    print ts_list
    
    print "\nfeaturize timeseries"
    row = pd.Series()
    for data_ID in all_data_ids:
        ftzer = BasicTSFeaturizer(data_ID)
        print ftzer.can_featurize(ts_list)
        featurized = ftzer.featurize(ts_list)
        print featurized
        row = row.append(featurized)
    row.name = hadm_id
    df = pd.DataFrame()
    df = df.append(row)
    
    print df

if __name__ == "__main__":
    main()
