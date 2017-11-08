import pandas as pd

class open_filename(object):
    """
    Context manager that opens a filename and closes it on exit, but does
    nothing for file-like objects.
    """
    def __init__(self, filename, *args, **kwargs):
        self.closing = kwargs.pop('closing', False)
        if isinstance(filename, str):
            self.fh = open(filename, *args, **kwargs)
            self.closing = True
        else:
            self.fh = filename

    def __enter__(self):
        self.fh.seek(0)
        return self.fh

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.closing:
            self.fh.close()
        else:
            self.fh.seek(0)

        return False

def sort_mixed_list(iterable):
    """
    Sort a list of strings and other objects:
    put the strings first and the others last

    Parameters
    ----------
    iterable : list

    Returns
    -------
    list
    """
    strings = []
    others = []
    for elem in iterable:
        if isinstance(elem, str):
            strings.append(elem)
        else:
            others.append(elem)
    res = strings + others
    return res

def date_range(start=None, end=None, periods=None, **kwargs):
    """
    Extension of pandas date range.
    Can handle start, end AND a number of periods

    Parameters
    ----------
    start : pd.Timestamp
    end : pd.Timestamp
    periods : int
    kwargs : kwargs

    Returns
    -------
    pd.DatetimeIndex
    """
    if start and end and periods:
        delta = end - start
        period = delta / (periods-1)
        dr = pd.date_range(start=start, end=end, freq=period)
    else:
        dr = pd.date_range(start=start, end=end, periods=periods, **kwargs)

    return dr