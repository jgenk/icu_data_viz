import admissions
DNE_CODE = "DNE"

class attribute_tag:
    GENDER = "GENDER"
    NAME = "NAME"
    DOB = "DATE OF BIRTH"
    DOD = "DATE OF DEATH"
    LANG = "LANGUAGE"
    RELIGION = "RELIGION"
    MARITAL_STATUS = "MARITAL STATUS"
    ETHNICIY = "ETHNICITY"

class patient:
    """A patient object. Each patient contains:
        1. A unique ID
        2. A dict of attributes, such as GENDER, NAME, etc. (See patients.attribute_tag)
        3. A list of admissions, sorted by admissions.admit_dt
        """

    def __init__(self, uniqid):
        self.uniqid = uniqid
        self.attributes = {}
        self.admissions = []
        self.temp_attr_map = {} # k = hamd_id, v = map of attribute_tag : value
        self.active_adm_id = None

    def add_attr(self, tag, value):
        """Returns old attribute value if replaced, otherwise None"""
        oldattr = self.attributes.get(tag, None)
        self.attributes[tag] = value
        return oldattr

    def add_admission(self, admission, temp_attr_dict):
        """Adds admission to this patients list of admission, sorts list by
        admit_dt, and returns the updated list of admission objects."""
        self.admissions.append(admission)
        self.admissions.sort(key=lambda admit_dt:admission.admit_dt)
        self.temp_attr_map[admission.hadm_id] = temp_attr_dict
        return self.admissions

    def set_active_admission(self, hadm_id):
        """Sets the current admission to  hadm_id, updates temporary attributes."""
        if not self.active_adm_id is None:
            for attr_key in self.temp_attr_map[self.active_adm_id].keys():
                self.attributes.pop(attr_key, None)
        for (attr_key,val) in self.temp_attr_map[hadm_id].iteritems():
            self.add_attr(attr_key,val)

        old_adm = self.active_adm_id
        self.active_adm_id = hadm_id
        return old_adm

    def get_most_recent_admission(self):
        return self.admissions[-1]

    def get_active_admission(self):
        for admission in self.admissions:
            if admission.hadm_id == self.active_adm_id : return admission
        return None

    def __str__(self):
        output = "Patient: " + str(self.uniqid) + '\n'
        for (key, value) in self.attributes.iteritems():
            output += '\t' + str(key) + " - " + str(value) + '\n'
        output += '\t' + "Total admissions: " + str(len(self.admissions)) + '\n'
        output += '\t' + "Active admission: " + str(self.get_active_admission())

        return output
