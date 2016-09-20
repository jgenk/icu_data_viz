class measurement:
    """docstring for measurement."""
    def __init__(self, name, unit, normal_range):
        self.name = name
        self.unit = unit
        self.normal_range = normal_range

    def in_normal_range(self, val):
        return self.normal_range.high >= val and self.normal_range.low <= val

    def __str__(self):
        return '{0.name} ({0.normal_range} {0.unit})'.format(self)

class measurement_trend:
    """docstring for measurement_trend."""
    def __init__(self, msrmt,values):
        self.msrmt = msrmt
        self.values = values # ([0][:] = timestamps (datetime), [1][:]) = values

class normal_range:
    def __init__(self, low, high):
        self.low = low
        self.high = high

    def __str__(self):
        return str(self.low)+ "-" + str(self.high)
