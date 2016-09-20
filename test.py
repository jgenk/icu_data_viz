import measurements as m
import patients
import mimic_psql_processor

PASSED_STRING = 'PASSED'
FAILED_STRING = 'FAILED'

def msrmnt_test():
    lactate = m.measurement("Lactate","mg/dL", m.normal_range(0,1.5))
    print '\n',lactate
    return lactate.in_normal_range(1)

def trend_test():
    return True

def pt_test():
    cur = mimic_psql_processor.mimic_psql_cur("mimic","postgres","123")
    patient_id = input('Enter patient ID: ')
    patient = mimic_psql_processor.build_patient(cur,patient_id)
    print patient
    return True


if __name__ == '__main__':
    print "Measurement Test: ", PASSED_STRING if msrmnt_test() else FAILED_STRING
    print "Trend Test: ", PASSED_STRING if trend_test() else FAILED_STRING
    print "Pt Test: ", PASSED_STRING if pt_test() else FAILED_STRING



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
