import patients
import psql_utils as pu
import admissions
import unit_stays
from processor import PatientProcessor

MIMIC_III = "mimiciii"

class mimic_tables:
    ADMISSIONS = "ADMISSIONS"
    PATIENTS = "PATIENTS"
    TRANSFERS = "TRANSFERS"

class mimic_columns:
    SUBJECT_ID =  "subject_id"
    HADM_ID = "hadm_id"
    ICUSTAY_ID = "icustay_id"



class MimicPatientProcessor(PatientProcessor):

    def __init__(self, mimic_dbname='mimic', user='postgres', pw='123'):
        self.psql_cur = pu.psql_cur(mimic_dbname,user,pw)
        PatientProcessor.__init__(self)


    def get_and_populate_pt_data(self, patient):
        """
        This method should population a patient object based on whatever data can be derived
        from the data source.

        return
            True    if successful
            False   if failed
        """

        build_mimic_patient(self.psql_cur, patient)

        return True

    def get_pt_ids(self):
        """
        Gets a list of patient IDs for this processor.

        return  list of patient ids that will be used initialize this processor
        """
        self.psql_cur.cur.execute(' '.join([pu.psql_str.SELECT,mimic_columns.SUBJECT_ID,
                                           pu.psql_str.FROM, MIMIC_III + "." + mimic_tables.PATIENTS]))
        return [row[mimic_columns.SUBJECT_ID] for row in self.psql_cur.cur.fetchall()]

    def init_pt_for_id(self, pt_id):
        pt_row = get_rows_for_pt(self.psql_cur.cur, pt_id, mimic_tables.PATIENTS)[0]
        return init_patient_from_row(pt_row)

    def close(self):
        self.psql_cur.close_dbconnect()
        return

def mimic_get_rows_for_ID(cur, ID, table_name, id_col_nm, columns='*'):
    """Applies MIMIC_III namespace to table_name"""
    return pu.get_rows_for_ID(cur,ID, MIMIC_III + "." + table_name,id_col_nm, columns)


def get_rows_for_pt(cur, subject_id, table_name, columns='*'):
    """Given a subject_ID and table name, identify the subject_ID column and
        retrieve all rows in that table related to the subject_ID."""

    return mimic_get_rows_for_ID(cur, subject_id, table_name, mimic_columns.SUBJECT_ID, columns)

def build_mimic_patient(psql_cur, patient):
    """Builds a patient based on the unique subject_id from patients table"""

    #1. Retrieve all admissions for patient from ADMISSIONS table and add to patient
    for admission_row in get_rows_for_pt(psql_cur.cur, patient.uniqid, mimic_tables.ADMISSIONS):
        #Init admission object
        admission_obj = init_admission_from_row(admission_row)
        #Add to patient
        patient.add_admission(admission_obj, get_temp_attr_for_admission(admission_row))

        #Populate unit stays
        for transfer_row in mimic_get_rows_for_ID(psql_cur.cur, admission_obj.hadm_id, mimic_tables.TRANSFERS, mimic_columns.HADM_ID):
            new_unit_stay = unit_stay_for_transfer_row(transfer_row)
            if new_unit_stay is None: continue
            admission_obj.add_unit_stay(new_unit_stay)
            #TABLES TO POPULATE UNIT STAY WITH:
            #           CHARTEVENTS
            #
            if new_unit_stay.is_ICU_stay() : populate_chart_events(psql_cur.cur, new_unit_stay)
            # us = unit_stay_from_transfer_row(transfer_row)
            # admission_obj.add_unit_stay(us)

    #2. Set the current admission to the most recent one
    patient.set_active_admission(patient.get_most_recent_admission().hadm_id)
    return patient

def init_patient_from_row(row):
    """Builds a patients.patient object with all attributes from the MIMIC_III table."""

    #init patient
    patient = patients.patient(row[mimic_columns.SUBJECT_ID])

    #Map column names in mimic iii PATIENTS table to attribute_tags
    attr_map = {'gender' : patients.attribute_tag.GENDER,
                'dob': patients.attribute_tag.DOB,
                'dod': patients.attribute_tag.DOD}

    for (col_nm,attr_tag) in attr_map.iteritems():
        patient.add_attr(attr_tag, row[col_nm])

    return patient

def init_admission_from_row(row):
    """Inits a admissions.admission skeleton."""

    admission_id = row[mimic_columns.HADM_ID]
    admission = admissions.admission(row[mimic_columns.HADM_ID],
                                     row['admittime'],
                                     row['dischtime'],
                                     row['diagnosis'],
                                     row['hospital_expire_flag'])
    return admission

def get_temp_attr_for_admission(row):
    """extract all temporary patient attributes that are documented for this admission;
    These might be different for different admissions, so patient has an attribute mask
    applied based on the "active" admission"""
    #Map column names in mimic iii ADMISSIONS table to attribute_tags
    attr_map = {'language' : patients.attribute_tag.LANG,
                'religion': patients.attribute_tag.RELIGION,
                'marital_status': patients.attribute_tag.MARITAL_STATUS,
                'ethnicity': patients.attribute_tag.ETHNICIY}
    adm_attr = {}

    for (col_nm,attr_tag) in attr_map.iteritems():
        adm_attr[attr_tag] = row[col_nm]

    return adm_attr

def unit_stay_for_transfer_row(row):
    """
    Returns a unit stay object for a given transfer row.
    """
    transfer_type = row['eventtype']
    if transfer_type != 'transfer': return None

    new_unit_stay = unit_stays.unit_stay(row['curr_wardid'], row['intime'], row['outtime'])

    if is_icu_row(row):
        new_unit_stay.set_ICU_info(row[mimic_columns.ICUSTAY_ID], row['curr_careunit'])

    return new_unit_stay

def is_icu_row(row):
    """checks for ICUSTAY_ID in a given row, indicating that it is in fact an ICU stay"""
    return (not row.get(mimic_columns.ICUSTAY_ID, None) is None)

def populate_chart_events(cur, icu_stay):
    return
