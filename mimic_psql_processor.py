import measurements
import patients
import psycopg2
import abc
import psql_utils
import admissions

MIMIC_III = "mimiciii"

class mimic_tables:
    ADMISSIONS = "ADMISSIONS"
    PATIENTS = "PATIENTS"

class mimic_columns:
    SUBJECT_ID =  "subject_id"
    HADM_ID = "hadm_id"

class psql:
    SELECT="SELECT"
    FROM="FROM"
    WHERE="WHERE"


class mimic_psql_cur(psql_utils.psql_cur):

    def get_rows_for_pt(self, subject_id, table_name, columns='*', id_col_nm=mimic_columns.SUBJECT_ID):
        """Given a subject_ID and table name, identify the subject_ID column and
            retrieve all rows in that table related to the subject_ID."""
        self.cur.execute(' '.join([psql.SELECT, columns,
                             psql.FROM, MIMIC_III + "." + table_name,
                             psql.WHERE, table_name + '.' + id_col_nm, "=", '\'{0}\''.format(subject_id)]))
        return self.cur.fetchall()



def build_patient(mimic_psql_cur, subject_id):
    """Builds a patient based on the unique subject_id from patients table"""

    #1. init patient with data from PATIENTS table"
    patient = init_patient_from_row(mimic_psql_cur.get_rows_for_pt(subject_id, mimic_tables.PATIENTS)[0])

    #2. Retrieve all admissions for patient from ADMISSIONS table and add to patient
    for admission_row in mimic_psql_cur.get_rows_for_pt(subject_id, mimic_tables.ADMISSIONS):
        patient.add_admission(init_admission_from_row(admission_row), get_temp_attr_for_admission(admission_row))

    #3. Set the current admission to the most recent one
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
