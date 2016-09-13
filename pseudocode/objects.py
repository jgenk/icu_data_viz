class MEASUREMENT:
  string name
  string unit
  (float, float) normal range

  def IN_NORMAL_RANGE(val)
    return bool

class MEASUREMENT_TREND:
  measurement msr
  list[2][n] data   # ([0][:] = timestamps (datetime), [1][:])

  def INFER_CONTINUOUS()
    return model of data w/ continuous representation

class PATIENT:
  string name
  int mrn
  list measurement_trend  # list vs. set
