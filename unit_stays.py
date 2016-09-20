

class unit_stay(object):
    def __init__(self, unitID, start, end):
            self.unit_id=unitID
            self.start = start
            self.end = end

    def sublist_for_stay(self):
        pass

class icu_stay(unit_stay):
    def __init__(self, unitID, start, end, icustay_id):
        super(icu_stay, self).__init__(self, unitID, start, end)
        self.icustay_id = icustay_id
