import measurements

class unit_stay(object):
    def __init__(self, unitID, start, end):
        self.unit_id = unitID
        self.start = start
        self.end = end
        self.icustay_id = None
        self.icu_name = None

    def sublist_for_stay(self, trend):
        return trend.values_for_range(start,end)

    def set_ICU_info(self, icustay_id, icu_name):
        self.icustay_id = icustay_id
        self.icu_name = icu_name

    def is_ICU_stay(self):
        return not self.icustay_id is None

    def get_lengthOfStay(self):
        return self.end -self.start
