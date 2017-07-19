import csv
from cached_property import cached_property
import pytz
import datetime as dt

from .misc import open_filename


class UNIBaseParser:
    def __init__(self, file):
        """
        Parameters
        ----------
        file : str | FileIO | StringIO
            file path, file or string buffer
        """
        self.file = file
        self.body_start_line = None
        self.body_end_line = None

        with open_filename(filename=file, mode='r') as f:
            self.raw = list(csv.reader(f, delimiter=";"))
        if len(self.raw) == 0:
            raise ValueError("Empty CSV-file")

        self.dict = self._parse_properties(raw=self.raw)

        self.df = None

    @property
    def properties(self):
        """
        Returns
        -------
        set
        """
        return set(self.dict.keys())

    def get_property(self, key):
        """
        Parameters
        ----------
        key : str

        Returns
        -------
        str | list(str) | None
        """
        return self.dict.get(key)

    @cached_property
    def timezone(self):
        """
        Get the timezone offset

        Returns
        -------
        pytz.FixedOffset
        """
        tz_string = self.get_property(key="Time zone")
        sign = tz_string[0]
        hours = int(tz_string[1:3])
        minutes = int(tz_string[3:5])

        offset = dt.timedelta(hours=hours, minutes=minutes)
        offset_minutes = offset.seconds / 60
        offset_signed = float(sign + str(offset_minutes))
        return pytz.FixedOffset(offset_signed)

    def _parse_properties(self, raw):
        """
        Transform raw lines into dict

        Parameters
        ----------
        raw : list(list(str))

        Returns
        -------
        dict
        """
        d = {}

        for i, line in enumerate(raw):
            if len(line) == 0:
                continue
            key = line[0]

            # all keys start and end with square braces
            if not key.startswith('['):
                continue  # skip this line
            else:
                key = key.strip("[]")

            if key == "Body Start":
                self.body_start_line = i + 1
                continue
            elif key == "Body End":
                self.body_end_line = i - 1
                continue

            # get everything after the key, but no empty strings
            value = [elem for elem in line[1:] if elem != '']

            if len(value) == 1:
                value = value[0]
            elif len(value) == 0:
                continue

            d.update({key: value})

        return d

    def get_dataframe(self):
        """
        Returns
        -------
        pd.DataFrame
        """
        if self.df is not None:
            return self.df
        else:
            return self._parse_dataframe()

    def get_timeseries_frame(self):
        """
        Returns
        -------
        pd.DataFrame

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError('Method needs to be implemented by subclass')

    def get_metadata_frame(self):
        """
        Returns
        -------
        pd.DataFrame

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError('Method needs to be implemented by subclass')

    def _parse_dataframe(self):
        """
        Returns
        -------
        pd.DataFrame

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError('Method needs to be implemented by subclass')
