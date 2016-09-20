import measurements as m
import patients
import mimic_psql_processor
from datetime import datetime,timedelta
import random

PASSED_STRING = 'PASSED'
FAILED_STRING = 'FAILED'

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

def pt_test():
    cur = mimic_psql_processor.mimic_psql_cur("mimic","postgres","123")
    patient = mimic_psql_processor.build_patient(cur,109)
    # print patient
    # for adm in patient.admissions :
    #     print adm
    return PASSED_STRING


if __name__ == '__main__':
    print "Measurement Test: ", msrmnt_test()
    print "Trend Test: ", trend_test()
    print "Pt Test: ", pt_test()



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
