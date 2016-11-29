from functools import total_ordering
from hashlib import sha1


class Patient(object):

    def __init__(self, pt_id):
        self.pt_id = pt_id
        self.attr_list = set()
        self.admission_attr_dict = {}
        self.admission_index = {}
        pass

    def add_attr(self, type, val):
        new_attr = PatientAttribute(type, val)
        self.attr_list.add(new_attr)
        return new_attr

    def add_admission(self, admission, attributes=[]):
        self.admission_attr_dict[admission.hadm_id]=attributes
        self.admission_index[admission.hadm_id] = admission
        return

    def get_admission(self, hadm_id):
        return self.admission_index(hadm_id)

    def get_attributes(self,hadm_id=None):
        temp_attr_list = set(self.attr_list)
        if not hadm_id is None : temp_attr_list.add(self.admission_attr_dict[hadm_id])
        return temp_attr_list

@total_ordering # still need to implement eq & one of lt/le/gt/ge
class PatientAttribute(object):

    def __init__(self, type, val):
        self.type = type
        self.value = val
        pass

    def __eq__(self, other):
        return self.type == other.type

    def __lt__(self, other):
        return self.type < other.type

    def __hash__(self):
        return sha1(self.type)

class AttributeType(object):
    GENDER = "GENDER"
    NAME = "NAME"
    DOB = "DATE OF BIRTH"
    DOD = "DATE OF DEATH"
    LANG = "LANGUAGE"
    RELIGION = "RELIGION"
    MARITAL_STATUS = "MARITAL STATUS"
    ETHNICIY = "ETHNICITY"

@total_ordering # still need to implement eq & one of lt/le/gt/ge
class Admission(object):
    def __init__(self, hadm_id):
        self.hadm_id = hadm_id

    def __eq__(self, other):
        return self.hadm_id == other.hadm_id

    def __lt__(self, other):
        return self.hadm_id < other.hadm_id
