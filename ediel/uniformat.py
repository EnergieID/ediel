"""Base class for parsing UNI files."""

import csv
import datetime as dt
import io
from typing import Optional, Union

import pandas as pd
import pytz
from cached_property import cached_property

from .misc import open_filename


class EmptyFileException(Exception):
    """Exception raised when the file is empty."""


class ParserError(Exception):
    """Exception raised when the parser encounters an error."""


class UNIBaseParser:
    """Base class for parsing UNI files."""

    date_format = "%d%m%Y %H:%M"

    def __init__(
        self,
        file: Union[str, io.StringIO, io.FileIO],
        file_name: Optional[str] = None,
        remove_contract_info_lines: bool = False,
    ):
        """
        file can be file path, fileIO or stringIO
        """
        self.file = file
        self.file_name = file_name

        with open_filename(filename=file, mode="r") as f:
            raw = list(csv.reader(f, delimiter=";"))

        body_lines_removed = 0
        if remove_contract_info_lines:
            new_raw = []
            for line in raw:
                if "CONTRACT-INFO" in line:
                    body_lines_removed += 1
                    continue
                new_raw.append(line)
        self.raw = new_raw if remove_contract_info_lines else raw
        self.strio = io.StringIO("\n".join(";".join(x) for x in self.raw))

        if len(self.raw) == 0:
            raise EmptyFileException

        self.dict = self._parse_properties(raw=self.raw)

        try:
            self.body_start_line = self.dict["Body Start"]
            self.body_end_line = self.dict["Body End"]
        except KeyError as e:
            raise ParserError(
                "Body is not clearly marked by Body Start and Body End"
            ) from e

        self.df = None

    @property
    def properties(self):
        """
        Returns
        -------
        set
        """
        return set(self.dict.keys())

    def get_property(self, key, default=None):
        """
        Parameters
        ----------
        key : str
        default : str | list(str) | None

        Returns
        -------
        str | list(str) | None
        """
        return self.dict.get(key, default)

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
        """Get the creation date of the file."""
        co = self.get_property(key="Created on")
        date_str = " ".join(co)
        if len(date_str) == 0:
            return pd.NaT
        created_on = pd.to_datetime(date_str, format=self.date_format)
        created_on = created_on.tz_localize(self.timezone)
        return created_on

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
            if not key.startswith("["):
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
            value = [elem for elem in line[1:] if elem != ""]

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
        raise NotImplementedError("Method needs to be implemented by subclass")

    def get_metadata_frame(self):
        """
        Returns
        -------
        pd.DataFrame

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError("Method needs to be implemented by subclass")

    def _parse_dataframe(self):
        """
        Returns
        -------
        pd.DataFrame

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError("Method needs to be implemented by subclass")

    def _date_parser(self, datetime_str: str) -> pd.Timestamp:
        try:
            parsed = pd.to_datetime(datetime_str, format="%d%m%Y %H:%M")
        except ValueError:
            return pd.NaT
        datetime = parsed.tz_localize(self.timezone)

        return datetime
