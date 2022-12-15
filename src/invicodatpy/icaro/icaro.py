#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write ICARO
"""

import os
import pandas as pd
from ..models.icaro_model import IcaroModel
from ..utils.sql_utils import SQLUtils
from sqlalchemy import create_engine
from ..utils.print_tidyverse import PrintTidyverse

class Icaro():
    "Some generals methods"
    df = None
    _TABLE_NAME = ''
    _INDEX_COL = None
    _FILTER_COL = ''

    # --------------------------------------------------
    def read_csv(
        self, PATH:str, names=None) -> pd.DataFrame:
        """"Read from csv SGF's report"""
        df = pd.read_csv(PATH, index_col=None, header=None, 
        na_filter = False, dtype=str, encoding = 'ISO-8859-1',
        on_bad_lines='warn', names=names)
        n_col = df.shape[1]
        df.columns = [str(x) for x in range(n_col)]
        return df

    # --------------------------------------------------
    def to_sql(self, sql_path:str, replace:bool = False):
        """From DataFrame to sql DataBase"""
        if not os.path.exists(sql_path):
            engine = IcaroModel(sql_path).engine
        else:
            engine = create_engine(f'sqlite:///{sql_path}')
        
        if replace:
            SQLUtils().delete_all_rows(
                engine, self._TABLE_NAME)            
        else:
            SQLUtils().delete_rows_with_df_col(
                engine, self.df, self._TABLE_NAME, self._FILTER_COL)
        
        self.df.to_sql(
            name = self._TABLE_NAME,
            con = engine,
            if_exists = 'append',
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