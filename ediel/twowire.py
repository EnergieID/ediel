from cached_property import cached_property
import pandas as pd

from .uniformat import UNIBaseParser


class TwoWireParser(UNIBaseParser):
    def __init__(self, file):
        """
        Parameters
        ----------
        file : str
        """
        super(TwoWireParser, self).__init__(file=file)

    @cached_property
    def interval(self):
        """
        Returns
        -------
        str
        """
        _format = self.get_property(key='Format')
        # looks like ['MMR', 'Interval: 5 min']
        # so we want the second element, last part, and without the space
        return _format[1].split(': ')[1].replace(' ', '')

    def _date_parser(self, date_str, time_str):
        """
        Parameters
        ----------
        date_str : str
        time_str : str

        Returns
        -------
        pd.Timestamp
        """
        parsed = pd.Timestamp.strptime(date_str + time_str, "%d%m%Y%H:%M")
        datetime = parsed.tz_localize(self.timezone)

        return datetime

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
    def __init__(self, file):
        """
        Parameters
        ----------
        file : str
        """
        super(TwoWireMMRParser, self).__init__(file=file)
        self.is_long_format = False

    def _parse_dataframe(self):
        """
        Returns
        -------
        pd.DataFrame
        """
        def pandas_read(parse_dates, column_names):
            _df = pd.read_csv(
                self.file,
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
                parse_dates=[[5, 6], [8, 9]],
                column_names={
                    0: 'Name',
                    1: 'Type',
                    2: 'Tariff',
                    3: 'Cumulative',
                    4: 'Unit',
                    '5_6': 'Start',
                    '8_9': 'End'
                }
            )
        except ValueError:
            df = pandas_read(
                parse_dates=[[6, 7], [9, 10]],
                column_names={
                    0: 'Ean',
                    1: 'Name',
                    2: 'Type',
                    3: 'Tariff',
                    4: 'Cumulative',
                    5: 'Unit',
                    '6_7': 'Start',
                    '9_10': 'End'
                }
            )
            self.is_long_format = True

        return df

    def get_timeseries_frame(self):
        """
        Returns
        -------
        pd.DataFrame
        """
        df = self.get_dataframe()

        start = df.Start.iloc[0]
        end = df.End.iloc[0]
        index = pd.date_range(start=start, end=end, freq=self.interval)

        if not self.is_long_format:
            vals = df[df.columns[6:]]
        else:
            vals = df[df.columns[7:-2]]

        vals = vals.T.dropna()
        vals.index = index

        return vals

    def get_metadata_frame(self):
        """
        Returns
        -------
        pd.DataFrame
        """
        df = self.get_dataframe()
        return df[df.columns[:6]].T
