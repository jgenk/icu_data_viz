import mimic_processor as mp
import random
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import matplotlib.cm as cm
import numpy as np
"""
"""
LACTATE = 50813

def test_NametoID(processor):
    name_query = raw_input("Name of variable:").strip()
    while len(name_query) > 0:
        var_id= processor.get_varid_for_name(name_query)
        print "Var_ID:",var_id
        name_query = raw_input("Name of variable:").strip()
    return

def test_idToName(processor):
    var_id = raw_input("ID of variable:").strip()
    while len(var_id) > 0:
        name = processor.get_name_for_varid(var_id)
        print "Name:",name
        var_id = raw_input("ID of variable:").strip()
    return

def test_ICUstays(processor,num_points=20,num_stays=3):
    var_id = int(raw_input("ID of variable to graph:").strip())
    num_points = int(raw_input("Minimum # points in series:").strip())
    num_stays = int(raw_input("# ICU Stays to display:").strip())
    ICU_ids = processor.list_icustay_ids()
    random.shuffle(ICU_ids)
    sick_stays = []

    for icustay_id in ICU_ids:
        icu_stay = processor.get_icu_stay_info(icustay_id)
        timeseries = processor.get_timeseries_for_icustay(icu_stay,[var_id])

        if len(timeseries[var_id]) > num_points:
            sick_stays.append(timeseries[var_id])
            print icu_stay

        if len(sick_stays) > num_stays: break


    # colors = np.random.rand(num_stays)
    color=cm.rainbow(np.linspace(0,1,len(sick_stays)))
    for i in range(0,len(sick_stays)):
        plot_timeseries(sick_stays[i],color[i])
    plt.xlim(xmin=0)
    plt.ylim(ymin=0)
    plt.show()
    return

def plot_timeseries(timeseries,color='red'):
    # dt = np.dtype([('datetime', dtype='datetime64[D]'),])
    timeseries = timeseries[timeseries[:,0].argsort()]
    timestamps = timeseries[:,0]
    timestamps = [((ts - min(timestamps)).total_seconds())/3600 + 1 for ts in timestamps]
    vals = timeseries[:,1]
    plt.scatter(timestamps,vals,c=color, alpha = 0.5)
    from scipy.interpolate import interp1d

    f2 = interp1d(timestamps, vals, kind='cubic')

    plt.plot(timestamps, vals, 'o', timestamps, f2(timestamps), '--')
    plt.legend(['data', 'cubic'], loc='best')
    return

if __name__ == '__main__':
    processor = mp.MimicProcessor()
    test_NametoID(processor)
    test_idToName(processor)
    test_ICUstays(processor)
