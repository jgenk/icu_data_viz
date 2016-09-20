import bisect

class normal_range:
    def __init__(self, low, high):
        self.low = low
        self.high = high

        def __str__(self):
            return str(self.low)+ "-" + str(self.high)

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
    def __init__(self, msrmt):
        self.msrmt = msrmt
        self.timestamps = [] # ordered list of timestamps (Datetime objects)
        self.values = [] # list of values, indicies matching corresponding timestamps

    def add_value(self, datetime, val, overwrite=True):
        """Adds a value to this trend at a specific datetime.
        If there is already a value at that datetime, it will be replaced unless overwrite=False.
        Returns the old value or None if there is no old value."""

        oldval = None
        try:
            index = self.timestamps.index(datetime)
            # if no exception, then this timestamp already exists
            oldval = self.values[index]
            # if we do not want to overwrite, quit now
            if not overwrite: return oldval
            self.timestamps[index] = datetime
            self.values[index] = val
        except ValueError:
            index = bisect.bisect(self.timestamps,datetime)
            self.timestamps.insert(index,datetime)
            self.values.insert(index,val)


        return oldval

    def len(self):
        return len(self.timestamps)

    def values_for_range(self, start_datetime=None, finish_datetime=None):
        """
        Returns the values of this measurement trend across a given time interval.
        Returned in the form of a 2 x n matrix, where [0,n] is datetime, [1,n] is value.
        __               __
        | t0 t1 t2 ... tn |
        | v0 v1 v2 ... vn |
        __               __


        """
        if start_datetime is None : start_datetime=self.timestamps[0]
        if finish_datetime is None : finish_datetime=self.timestamps[-1]
        start_index = bisect.bisect_left(self.timestamps,start_datetime)

        finish_index = bisect.bisect_right(self.timestamps,finish_datetime)
        out_matrix = [[],[]]
        for i in range(start_index, finish_index):
            out_matrix[0].append(self.timestamps[i])
            out_matrix[1].append(self.values[i])

        return out_matrix

    def __str__(self):
        output = '\n'.join(
                    '{0}\t{1}'.format(str(self.timestamps[i]),str(self.values[i]))
                        for i in range(0,len(self.timestamps)))

        return output
