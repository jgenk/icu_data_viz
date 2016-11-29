
from patient import Patient
import abc

class ICUDataProcessor(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        return


    @abc.abstractmethod
    def get_incustayIDs_for_criteria(self,var_id,mag=1,time_s=0):
        """
        For a given piece of data, a magnitude of change, and a time over which that change occurs,
        identify all icustays which meet the criteria.
        Examples
            Fluid bolus: ID = type of fluid, mag = volume given, time = time over which volume given
            Procedural intervention:    ID = type of procedure, mag=1 (0 to 1), time=instant (None)
        """
        return

    @abc.abstractmethod
    def get_timeseries_for_varid(self, var_id,pt_id,start_dttime,end_dttime):
        return

    @abc.abstractmethod
    def get_icu_stay_info(self, icustay_id):
        """
        return ID,unit_ID,start_dttime,end_dttime,patient
        """
        return

    @abc.abstractmethod
    def get_varid_for_name(self, name):
        return

    @abc.abstractmethod
    def get_name_for_varid(self, var_id):
        return

    @abc.abstractmethod
    def list_icustay_ids(self):
        return


    def get_all_timeseries_for_varname(self, name):
        return self.get_all_timeseries_for_varid(self.get_varid_for_name(name))

    def get_all_icu_stays(self, duration=None, pt_id=None, outcome=None):
        icu_list = []
        for icustay_id in self.list_icustay_ids():
            icu_stay = get_icu_stay_info(icustay_id)
            if meets_criteria(icu_stay,duration,pt_id,result): icu_list.append()
        return icu_list

    def get_timeseries_for_icustay(self, icustay,var_ids):
        var_dict = {}
        for var_id in var_ids:
            var_dict[var_id]=self.get_timeseries_for_varid(var_id,icustay.patient.id,icustay.start_dttime,icustay.end_dttime)
        return var_dict

class ICUStay(object):

    def __init__(self,ID,unit_ID,start_dttime,end_dttime,patient,outcome):
        self.id=ID
        self.unit_ID=unit_ID
        self.start_dttime=start_dttime
        self.end_dttime=end_dttime
        self.patient=patient
        self.outcome=outcome

    def duration(self):
        return self.end_dttime-start_dttime

    def __str__(self):
        return "#{id}, {unit_ID}: {start_dttime}-{end_dttime}\nPatient:{patient}\n{outcome}".format(**self.__dict__)

class ICUOutcome(object):
    DECEASED = "DECEASED"
    TRANSFERRED = "TRANSFERRED"
    DISCHARGED = "DISCHARGED"


def meets_criteria(icu_stay,duration=None,pt_id=None,outcome=None):
    if (duration is not None) and (icu_stay.duration() < duration): return False
    elif (pt_id is not None) and (icu_stay.pt.id != pt_id): return False
    elif (result is not None) and (icu_stay.outcome != outcome): return False
    return True
