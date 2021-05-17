import pandas as pd
from typing import List, Dict, Type, Iterator
import re

from .uniformat import UNIBaseParser
from .misc import open_filename
from .filename import match_filename

def parse_mig_file(filepath: str) -> 'MigParser':
    parser_cls = get_mig_parser_cls(filepath=filepath)
    parser = parser_cls(file=filepath)
    return parser

def get_mig_parser_cls(filepath: str) -> Type['MigParser']:
    r = match_filename(filename=filepath)
    if not r:
        raise ValueError(f'Not a valid MIG file: {filepath}')

    parsers = {
        '91': Mig3Export91Parser,
        '92': Mig3Export92Parser,
        '93': Mig3Export93Parser,
        '94': Mig3Export94Parser,
        '95': Mig3Export95Parser,
        '96': Mig3Export96Parser
    }
    parser = parsers[r['export_no']]
    return parser

class MigParser(UNIBaseParser):
    date_columns: List[int]  = NotImplemented
    column_names: Dict = NotImplemented

    def _parse_dataframe(self) -> pd.DataFrame:
        with open_filename(filename=self.file, mode='r') as f:
            df = pd.read_csv(
                filepath_or_buffer=f,
                sep=';',
                header=None,
                engine='python',
                skiprows=self.body_start_line,
                parse_dates=self.date_columns,
                date_parser=self._date_parser,
                decimal=',',
                skipfooter=len(self.raw) - self.body_end_line - 1
            )
        df.rename(columns=self.column_names, inplace=True)
        if 'Description' in df.columns:
            df['Description'] = df['Description'].str.strip(' ')

        return df


class Mig3Export91Parser(MigParser):
    date_columns = [0, 1]
    column_names = {
        0: 'Start',
        1: 'End',
        2: 'AccessEAN',
        3: 'Serial',
        4: 'CounterID',
        5: 'EnergyType',
        6: 'Direction',
        7: 'Unit',
        8: 'Reason',
        209: 'Interval',
        210: 'Description',
        211: 'CityEAN',
        212: 'GasConversionFactor',
        213: 'GasConversionUnit',
        214: 'GasConversionFactorQuality',
        215: 'RequestSenderRef',
        216: 'RequestReceiverRef'
    }

    def get_timeseries_frame(self) -> pd.DataFrame:
        df = self.get_dataframe()

        def parse_columns(frame: pd.DataFrame) -> Iterator[pd.DataFrame]:
            for _, group in frame.groupby(['AccessEAN', 'EnergyType', 'Unit']):
                parsed_rows = (self._parse_row_to_timeseries(row) for _, row in group.iterrows())
                column = pd.concat(parsed_rows)
                yield column
        columns = parse_columns(frame=df)
        df_t = pd.concat(columns, axis=1)
        return df_t

    @staticmethod
    def _parse_row_to_timeseries(row: pd.Series) -> pd.DataFrame:
        interval = row['Interval']

        index = pd.date_range(start=row.Start, end=row.End, freq=f'{interval}min', closed='right')
        step = int(5 - (60 / interval))
        start_slice = 9 + step - 1

        values = pd.Series(data=row[start_slice:start_slice + len(index) * step:step].values, index=index)
        meta_v = (row['AccessEAN'], row['Description'], row['EnergyType'],
                  row['Unit'], 'value')

        quality_codes = pd.Series(data=row[start_slice + 100:start_slice + 100 + len(index) * step: step].values,
                                  index=index)
        meta_q = (row['AccessEAN'], row['Description'], row['EnergyType'],
                  row['Unit'], 'quality')

        # if the quality code equals "?", we want the value to result in NaN
        # by multiplying it with float('NaN')
        # if the quality code is anything else, we retain it by multiplying it with 1
        quality_multiplier = quality_codes.map(lambda x: float('NaN') if x == '?' else 1)
        values = values * quality_multiplier

        ts = pd.concat([values, quality_codes], axis=1)
        ts.columns = pd.MultiIndex.from_tuples([meta_v, meta_q])
        return ts


class Mig3Export92Parser(Mig3Export91Parser):
    pass


class Mig3Export93Parser(Mig3Export91Parser):
    pass


class Mig3Export94Parser(MigParser):
    calculated_columns = [
        'AccessEAN',
        'Serial',
        'RegisterID',
        'EnergyType',
        'TimeFrame',
        'Start',
        'End',
        'QualityCode',
        'QualityReason',
        'Unit',
        'Reason',
        'Value',
        'Estimate',
        'EstimateStart',
        'SwitchingCategory',
        'Description',
        'CityEAN',
        'Blank1',
        'Blank2'
    ]

    phyisical_columns = [
        'AccessEAN',
        'Serial',
        'RegisterID',
        'EnergyType',
        'MeteringMethod',
        'Unit',
        'TimeFrame',
        'PreviousDateTime',
        'PreviousValue',
        'PreviousQualityCode',
        'PreviousQualityReason',
        'LatestDateTime',
        'LatestValue',
        'LatestQualityCode',
        'LatestQualityReason',
        'Reason',
        'Description',
        'MeterType',
        'GasConversionFactor',
        'GasConversionUnit',
        'Blank1',
        'Blank2'
    ]

    num_cols = ['Value', 'Estimate', 'PreviousValue', 'LatestValue', 'GasConversionFactor']
    datetime_cols = ['Start', 'End', 'EstimateStart', 'PreviousDateTime', 'LatestDateTime']

    def _parse_dataframe(self) -> pd.DataFrame:
        with open_filename(filename=self.file, mode='r') as f:
            df = pd.read_csv(
                filepath_or_buffer=f,
                engine='python',
                header=None,
                skiprows=self.body_start_line,
                skipfooter=len(self.raw) - self.body_end_line - 1,
                sep="|",
                names=['row']
            )

        df['calculated'] = df.row.apply(self._is_calculated_row)

        df_calculated = df[df['calculated']].copy()
        df_physical = df[~df['calculated']].copy()

        df_calculated[self.calculated_columns] = df_calculated['row'].str.split(';', n=18, expand=True)
        df_physical[self.phyisical_columns] = df_physical['row'].str.split(';', n=21, expand=True)

        df = pd.concat([df_calculated, df_physical], sort=False)
        df.drop(axis=1, columns=['row', 'Blank1', 'Blank2'], inplace=True)

        df.replace('', float('NaN'), inplace=True)

        for col in self.num_cols:
            try:
                df[col] = df[col].str.replace(',', '.')
            except AttributeError:  # entire row empty
                pass

            df[col] = df[col].astype(float)

        for col in self.datetime_cols:
            df[col] = df[col].apply(self._date_parser)

        if 'Description' in df.columns:
            df['Description'] = df['Description'].str.strip(' ')

        return df

    @staticmethod
    def _is_calculated_row(row: str) -> bool:
        """Checks if the row concerns a calculated or physical register"""
        match = re.match(string=row, pattern="[0-9]{18};AP LEVEL;.*")
        return match is not None

    def _date_parser(self, datetime_str: str) -> pd.Timestamp:
        """Override the date parser to deal with NaN's"""
        if not isinstance(datetime_str, str):
            return pd.Timestamp('NaT')
        else:
            return super(Mig3Export94Parser, self)._date_parser(datetime_str=datetime_str)

    def get_timeseries_frame(self) -> pd.DataFrame:
        df = self.get_dataframe()
        values = df[df['calculated'] & (df['Unit'] == 'KWH')].groupby(['AccessEAN', 'Description', 'Start', 'End']).Value.sum()
        values = values.unstack(level=[0,1])

        return values

class Mig3Export95Parser(MigParser):
    date_columns = [0, 1]
    column_names = {
        0: 'Start',
        1: 'End',
        2: 'AccessEAN',
        3: 'EnergyType',
        4: 'MeteringMethod',
        5: 'TimeFrame',
        6: 'Direction',
        7: 'Unit',
        8: 'Reason',
        9: 'Consumption',
        10: 'QualityCode',
        11: 'Description'
    }

class Mig3Export96Parser(Mig3Export95Parser):
    pass