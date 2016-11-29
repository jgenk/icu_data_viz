

class Patient(object):

    def __init__(self,ID,dob,gender,ethnicity):
        self.id = ID
        self.dob = dob
        self.gender = gender
        self.ethnicity = ethnicity

    def __str__(self):
        return "{id}, {dob}, {gender}, {ethnicity}".format(**self.__dict__)
