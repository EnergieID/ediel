from cached_property import cached_property
import pandas as pd
from typing import Union, List, Dict, Tuple, Optional
import io

from .uniformat import UNIBaseParser
from .misc import open_filename, sort_mixed_list, date_range


def get_parser(path_or_buff: Union[str, io.StringIO, io.FileIO]) -> 'TwoWireParser':
    pre_parser = TwoWireParser(path_or_buff)
    if pre_parser.format == 'MMR':
        return TwoWireMMRParser(path_or_buff)
    elif pre_parser.format == 'AMR':
        return TwoWireAMRParser(path_or_buff)
    else:
        raise ValueError(f'Unknown file format: {pre_parser.format}')


class TwoWireParser(UNIBaseParser):
    @cached_property
    def format(self) -> str:
        _format = self.get_property(key='Format')
        if isinstance(_format, str):
            return _format
        else:
            return _format[0]

    @cached_property
    def interval(self):
        """
        Returns
        -------
        str
        """
        _format = self.get_property(key='Format')
        if isinstance(_format, str):  # this means no interval is specified
            return None
        else:
            # looks like ['MMR', 'Interval: 5 min']
            # so we want the second element, last part, and without the space
            return _format[1].split(': ')[1].replace(' ', '')

    @property
    def val_range(self) -> Tuple[int, Optional[int]]:
        raise NotImplementedError

    @property
    def meta_range(self) -> Tuple[Optional[int], int]:
        raise NotImplementedError

    def get_timeseries_frame(self, allow_duplicate_names: bool = True) -> pd.DataFrame:
        """
        Parameters
        ----------
        allow_duplicate_names : bool
            default True
            if False, only the first occurence of a name is returned

        Returns
        -------
        pd.DataFrame
        """
        df = self.get_dataframe()

        val_from, val_to = self.val_range
        vals = df[df.columns[val_from:val_to]]

        if not allow_duplicate_names:
            vals = vals.reset_index()
            vals = vals.drop_duplicates(subset='Name')
            vals = vals.set_index('Name')

        vals = vals.T
        vals.dropna(inplace=True)

        start = df.Start.iloc[0]
        end = df.End.iloc[0]
        if self.interval is not None:
            index = date_range(start=start, end=end, freq=self.interval)
        else:
            num_periods = len(vals)
            index = date_range(start=start, end=end, periods=num_periods)

        vals.index = index

        return vals

    def get_metadata_frame(self, allow_duplicate_names=True):
        """
        Parameters
        ----------
        allow_duplicate_names : bool
            default True
            if False, only the first occurence of a name is returned

        Returns
        -------
        pd.DataFrame
        """
        df = self.get_dataframe()
        meta_from, meta_to = self.meta_range
        meta = df[df.columns[meta_from:meta_to]]

        if not allow_duplicate_names:
            meta = meta.reset_index()
            meta = meta.drop_duplicates(subset='Name')
            meta = meta.set_index('Name')

        return meta.T

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

    def _pandas_read(self, parse_dates: List[int], column_names: Dict):
        with open_filename(filename=self.file, mode='r') as f:
            _df = pd.read_csv(
                f,
                # because C parse does not support skipfooter:
                engine='python',
                header=None,
                skiprows=self.body_start_line,
                skipfooter=len(self.raw) - self.body_end_line - 1,
                sep=";",
                decimal=",",
                true_values=['yes'],
                false_values=['no'],
                parse_dates=parse_dates,
                date_parser=self._date_parser
            )
        _df.rename(columns=column_names, inplace=True)
        _df.set_index('Name', inplace=True)

        sorted_cols = sort_mixed_list(_df.columns.tolist())
        _df = _df[sorted_cols]

        return _df


class TwoWireMMRParser(TwoWireParser):
    def __init__(self, file: Union[str, io.StringIO, io.FileIO]):
        super(TwoWireMMRParser, self).__init__(file=file)
        self.is_long_format = False

    def _parse_dataframe(self):
        """
        Returns
        -------
        pd.DataFrame
        """
        try:
            df = self._pandas_read(
                parse_dates=[5, 7],
                column_names={
                    0: 'Name',
                    1: 'Type',
                    2: 'Tariff',
                    3: 'Cumulative',
                    4: 'Unit',
                    5: 'Start',
                    7: 'End'
                }
            )
        except ValueError:
            df = self._pandas_read(
                parse_dates=[6, 8],
                column_names={
                    0: 'Ean',
                    1: 'Name',
                    2: 'Type',
                    3: 'Tariff',
                    4: 'Cumulative',
                    5: 'Unit',
                    6: 'Start',
                    8: 'End'
                }
            )
            self.is_long_format = True

        return df

    @property
    def val_range(self) -> Tuple[int, Optional[int]]:
        if not self.is_long_format:
            return 6, None
        else:
            return 7, -2

    @property
    def meta_range(self) -> Tuple[Optional[int], int]:
        if not self.is_long_format:
            return None, 6
        else:
            return None, 7


class TwoWireAMRParser(TwoWireParser):
    @property
    def val_range(self) -> Tuple[int, Optional[int]]:
        return 5, 102

    @property
    def meta_range(self) -> Tuple[Optional[int], int]:
        return None, 5

    def _parse_dataframe(self) -> pd.DataFrame:
        df = self._pandas_read(
            parse_dates=[0, 1],
            column_names={
                0: 'Start',
                1: 'End',
                2: 'Ean',
                3: 'Name',
                4: 'Type',
                5: 'Unit'
            }
        )
        return df

    def get_timeseries_frame(self, allow_duplicate_names: bool = True) -> pd.DataFrame:
        df = super(TwoWireAMRParser, self).get_timeseries_frame(allow_duplicate_names=allow_duplicate_names)
        df.drop(df.last_valid_index(), inplace=True)
        return df
