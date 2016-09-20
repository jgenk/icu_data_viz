import unit_stays

class admission:
    """
    Key information:
    1. Admit dt, dc/expired dt
    2. List of units, sorted by transferred-to time
    3. Categorization of admission
        CPT vs. ICD codes

    """
    def __init__(self, hadm_id, admit_dt, dc_dt, initial_dx="", expired_flag=False):
        self.hadm_id = hadm_id
        self.admit_dt = admit_dt
        self.dc_dt = dc_dt
        self.initial_dx=initial_dx
        self.expired_flag = expired_flag #TODO: is this necessary?
        self.unit_stays = []


    def add_unit_stay(self, unit_stay_obj):

        self.unit_stays.append(admission)
        self.unit_stays.sort(key=lambda start:unit_stay.start)
        return self.unit_stays

    def get_unit_stays(self):
        return self.unit_stays

    def __str__(self):

        return "HADM ID #{0} ({1}-{2}) Dx:{3}".format(self.hadm_id,
                                                          self.admit_dt.strftime('%Y/%m/%d'),
                                                          self.dc_dt.strftime('%Y/%m/%d'),
                                                          self.initial_dx)
