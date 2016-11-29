import measurements as m
import patients
import psql_utils
import mimic_psql_processor as mpp
from datetime import datetime,timedelta
import random

PASSED_STRING = 'PASSED'
FAILED_STRING = 'FAILED'

class MiniMimicProcessor(mpp.MimicPatientProcessor):

    def __init__(self, pt_id_list):
        self.pt_id_list = pt_id_list
        mpp.MimicPatientProcessor.__init__(self)

    def get_pt_ids(self):
        return self.pt_id_list

def msrmnt_test():
    lactate = m.measurement("Lactate","mg/dL", m.normal_range(0,1.5))
    if lactate.in_normal_range(1) : return PASSED_STRING
    return FAILED_STRING

def trend_test():
    lactate = m.measurement("Lactate","mg/dL", m.normal_range(0,1.5))
    lactate_trend = m.measurement_trend(lactate)
    now = datetime.now()
    #1. random data set, test to confirm ordering effectively
    for i in range(0,100):
        timestamp = now + timedelta(hours=random.randint(0,48))
        old_val = lactate_trend.add_value(timestamp, 0.1 * random.randint(1,100))
        if lactate_trend.timestamps[-1] < lactate_trend.timestamps[0]: return 'timestamps not ordered properly'

    #2. Set of values, each 1 hour apart; confirm sub-list generation is appropriate
    lactate_trend = m.measurement_trend(lactate)
    for i in range(0,24):
        timestamp = now + timedelta(hours=i)
        lactate_trend.add_value(timestamp, 0.1 * i)
    trim = 2 #trim X values off of the list (24-x = len)
    subset = lactate_trend.values_for_range(None,now + timedelta(hours=(23-trim)))
    if not (len(subset[0]) == (lactate_trend.len()-trim)) : return 'Measurement trend not properly trimming subset.'

    return PASSED_STRING

def pt_test(pt_ids):
    processor = MiniMimicProcessor(pt_ids)

    # try:
    for pt_id in pt_ids:
        print "\nInit pt #", pt_id, ":", str(processor.get_patient(pt_id,False)) # do not build patient
        print "Build pt #", pt_id, ":", str(processor.get_patient(pt_id)) # Build patient
    # except Exception as e:
        # print e
        # return FAILED_STRING


    processor.close()

    return PASSED_STRING


if __name__ == '__main__':
    print "Measurement Test: ", msrmnt_test()
    print "Trend Test: ", trend_test()
    print "Little Pt Test: ", pt_test([95868,11318])
    # print "Pt Test: ", pt_test(109)




"""
mimic=# select subject_id,count(*) as cnt from mimiciii.admissions group by subject_id order by cnt desc limit 10;
    subject_id | cnt
    ------------+-----
    13033 |  42
    11861 |  34
    109 |  34
    5060 |  31
    20643 |  24
    19213 |  23
    7809 |  22
    5727 |  21
    23657 |  20
    11318 |  19
"""
