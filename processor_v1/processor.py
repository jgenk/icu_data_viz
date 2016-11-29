import abc
import patients

class PatientProcessor(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):

        self.init_patient_list()

    def init_patient_list(self):
        self.patient_dict = {}
        self.patient_built_status = {}
        for pt_id in self.get_pt_ids():
            self.add_patient(pt_id)
        return self.patient_dict

    def get_pt_list(self):
        return self.patient_dict.values()

    def add_patient(self, pt_id, should_build=False):
        """
        Adds a new patient to this processor by an id. Will replace existing patient
        with this same id. this will set up all indices properly

        args
            pt_id                uniq identifier of the patient
            should_build (False) if patient should immediately be built

        return  new patient object created with addition
        """
        new_pt = self.init_pt_for_id(pt_id)
        self.patient_dict[pt_id] = new_pt
        self.patient_built_status[pt_id] = False
        if should_build : self.build_patient(pt_id, new_pt)
        return new_pt

    def get_patient(self, pt_id, should_build=True, rebuild=False):
        patient = self.patient_dict.get(pt_id, None)
        if (not patient is None) and should_build :
            self.build_patient(pt_id, patient, rebuild)

        return patient

    def is_built(self, pt_id):
        """
        Checks if a patient has already been "built" i.e. all patient data is already in memory.

        return
            True    patient already built
            False   patient not already built
            None    patient ID does not exist
        """
        return self.patient_built_status.get(pt_id, None)

    def build_patient(self, pt_id, patient=None, rebuild=False):
        """
        Builds a patient if the patient based on the given data source.
        args
            pt_id               uniq ID of patient, patient OBJECT arg gets precedence
            patient             patient OBJECT, pt_id not required if present
            rebuild (False)     will force a patient to be rebuilt from scratch if True

        return
            True    patient successfully built
            False   patient not built; this may be because: patient w/ pt_id DNE
                                                            patient build failed
                                                            patient already built & rebuild == False
        """
        # will minimize dictionary queries if patient has already been pulled
        if patient is None:
            patient = self.patient_dict.get(pt_id, None)

        # Check if build CAN and SHOULD occur
        if (patient is None) or (self.is_built(patient.uniqid) and not rebuild) :
            return False

        # If we should rebuild this patient, start from scratch including re-initializing
        if rebuild :
            patient = self.add_patient(pt_id, False)

        #populate patient data
        successful = self.get_and_populate_pt_data(patient)
        self.patient_built_status[pt_id] = successful

        return successful


    @abc.abstractmethod
    def get_and_populate_pt_data(self, patient):
        """
        This method should population a patient object based on whatever data can be derived
        from the data source.

        return
            True    if successful
            False   if failed
        """
        return

    @abc.abstractmethod
    def get_pt_ids(self):
        """
        Gets a list of patient IDs for this processor.

        return  list of patient ids that will be used initialize this processor
        """
        return

    @abc.abstractmethod
    def init_pt_for_id(self, pt_id):
        """
        Create a patient object based on this pt_id for this specific processor.
        This method DOES NOT add pt to processor or any indices.

        return  new patient object
        """
        return

    @abc.abstractmethod
    def close(self):
        """
        Closes and cleans up anything stored in this processor. This icludes opened files,
        database connections, etc.

        return  <no return>
        """
        return
