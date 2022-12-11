#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF
"""

import os
import pandas as pd
from ..models.siif_model import SIIFModel
from ..utils.sql_utils import SQLUtils
from sqlalchemy import create_engine
from ..utils.print_tidyverse import PrintTidyverse

class SIIF():
    "Some generals methods"
    df = None
    _TABLE_NAME = ''
    _INDEX_COL = ''
    _FILTER_COL = ''

    # --------------------------------------------------
    def read_xls(self, PATH:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = pd.read_excel(PATH, index_col=None, header=None, 
        na_filter = False, dtype=str)
        n_col = df.shape[1]
        df.columns = [str(x) for x in range(n_col)]
        return df

    # --------------------------------------------------
    def to_sql(self, sql_path:str):
        """From DataFrame to sql DataBase"""
        if not os.path.exists(sql_path):
            engine = SIIFModel(sql_path).engine
        else:
            engine = create_engine(f'sqlite:///{sql_path}')
        SQLUtils().delete_rows_with_df_col(
            engine, self.df, self._TABLE_NAME, self._FILTER_COL)
        self.df.to_sql(
            name = self._TABLE_NAME,
            con = engine,
            if_exists='append',
            index=False
        )
        engine.dispose()

    # --------------------------------------------------
    def from_sql(self, sql_path:str) -> pd.DataFrame:
        """From sql DataBase to sql DataFrame"""
        engine = create_engine(f'sqlite:///{sql_path}')
        self.df = pd.read_sql_table(
            table_name = self._TABLE_NAME,
            con = engine,
            index_col = self._INDEX_COL
        )
        engine.dispose()
        return self.df

    # --------------------------------------------------
    def print_tidyverse(self):
        print(PrintTidyverse(self.df))

    # --------------------------------------------------
    def test_sql(self, sql_path:str):
        engine = create_engine(f'sqlite:///{sql_path}')
        self.df.to_sql(
            name = 'test',
            con = engine,
            if_exists='replace',
            index=False
        )
        engine.dispose()
        print('Sqlite test done')

