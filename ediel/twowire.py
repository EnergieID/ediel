from cached_property import cached_property
import pandas as pd
from typing import Union
import io

from .uniformat import UNIBaseParser
from .misc import open_filename, sort_mixed_list, date_range


class TwoWireParser(UNIBaseParser):
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
        def pandas_read(parse_dates, column_names):
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
            return _df

        try:
            df = pandas_read(
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
            df = pandas_read(
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

        sorted_cols = sort_mixed_list(df.columns.tolist())
        df = df[sorted_cols]
        return df

    def get_timeseries_frame(self, allow_duplicate_names=True):
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

        if not self.is_long_format:
            vals = df[df.columns[6:]]
        else:
            vals = df[df.columns[7:-2]]
            
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
        if not self.is_long_format:
            meta = df[df.columns[:6]]
        else:
            meta = df[df.columns[:7]]

        if not allow_duplicate_names:
            meta = meta.reset_index()
            meta = meta.drop_duplicates(subset='Name')
            meta = meta.set_index('Name')

        return meta.T
