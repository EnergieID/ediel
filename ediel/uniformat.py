import csv
from cached_property import cached_property
import pytz
import datetime as dt
from typing import Union
import io
import pandas as pd

from .misc import open_filename


class EmptyFileException(Exception):
    pass

class ParserError(Exception):
    pass


class UNIBaseParser:
    def __init__(self, file: Union[str, io.StringIO, io.FileIO]):
        """
        file can be file path, fileIO or stringIO
        """
        self.file = file

        with open_filename(filename=file, mode='r') as f:
            self.raw = list(csv.reader(f, delimiter=";"))
        if len(self.raw) == 0:
            raise EmptyFileException

        self.dict = self._parse_properties(raw=self.raw)

        try:
            self.body_start_line = self.dict['Body Start']
            self.body_end_line = self.dict['Body End']
        except KeyError:
            raise ParserError('Body is not clearly marked by Body Start and Body End')

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

    @cached_property
    def created_on(self) -> pd.Timestamp:
        co = self.get_property(key='Created on')
        date_str = ' '.join(co)
        return self._date_parser(datetime_str=date_str)

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
                d.update({key: i + 1})
                continue
            elif key == "Body End":
                d.update({key: i - 1})
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

    def _date_parser(self, datetime_str: str) -> pd.Timestamp:
        parsed = pd.to_datetime(datetime_str, format="%d%m%Y %H:%M")
        datetime = parsed.tz_localize(self.timezone)

        return datetime
