#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SQL methods
"""


__all__ = ['SQLUtils']

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import MetaData, Table, and_, create_engine, delete, engine


@dataclass
class SQLUtils():
    "Some generals methods"
    df = None
    _TABLE_NAME = ''
    _INDEX_COL = ''
    _FILTER_COL = ''
    _SQL_MODEL = None
    
    # --------------------------------------------------
    def from_external_report(self):
        """To be defined on specific modules"""
        pass

    # --------------------------------------------------
    """Delete rows from a table with one or multiple conditions"""
    def delete_rows_with_df_col(self, sql_path:str):
        self.engine = self._SQL_MODEL(sql_path).engine 
        connection = self.engine.connect()
        metadata = MetaData()
        sql_table = Table(self._TABLE_NAME, 
        metadata, autoload=True, autoload_with=self.engine)
        
        if isinstance(self._FILTER_COL, list):
            where_lst = []
            for i in self._FILTER_COL:
                unique_col = self.df[i].unique()
                where_lst.append(sql_table.c[i].in_(unique_col))
            where_clause = and_(*where_lst)
        else:
            unique_col = self.df[self._FILTER_COL].unique()
            where_clause = sql_table.c[self._FILTER_COL].in_(unique_col)
        
        u = delete(sql_table).where(where_clause)
        result = connection.execute(u)
        return result

    # --------------------------------------------------
    def delete_all_rows(self, sql_path:str):
        """Delete all rows from a table"""
        self.engine = self._SQL_MODEL(sql_path).engine   
        metadata = MetaData()
        sql_table = Table(self._TABLE_NAME, 
        metadata, autoload=True, autoload_with=self.engine)
        connection = self.engine.connect()
        u = delete(sql_table)
        result = connection.execute(u)
        return result

    # --------------------------------------------------
    def to_sql(self, sql_path:str, replace:bool = False):
        """From DataFrame to sql DataBase"""     
        if replace:
            self.delete_all_rows(sql_path)            
        else:
            self.delete_rows_with_df_col(sql_path)
        
        self.df.to_sql(
            name = self._TABLE_NAME,
            con = self.engine,
            if_exists = 'append',
            index=False
        )
        self.engine.dispose()

    # --------------------------------------------------
    def from_sql(self, sql_path:str, table_name:str = None) -> pd.DataFrame:
        """From sql DataBase to sql DataFrame"""
        engine = create_engine(f'sqlite:///{sql_path}')
        if table_name is None:
            table_name = self._TABLE_NAME
        self.df = pd.read_sql_table(
            table_name = table_name,
            con = engine,
            index_col = self._INDEX_COL
        )
        engine.dispose()
        return self.df

    # --------------------------------------------------
    def from_mdb(self, mdb_path:str, table_name:str = None) -> pd.DataFrame:
        """From mdb DataBase to sql DataFrame
        Package requirement:
            -   pip install sqlalchemy-access
        """
        connection_string = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ=" + mdb_path +
            r";"
            r"ExtendedAnsiSQL=1;"
        )

        connection_url = engine.URL.create(
            "access+pyodbc",
            query={"odbc_connect": connection_string}
        )
        
        engine_mdb = create_engine(connection_url)
        if table_name is None:
            table_name = self._TABLE_NAME
        self.df = pd.read_sql_table(
            table_name = table_name,
            con = engine_mdb,
            index_col = self._INDEX_COL
        )
        engine_mdb.dispose()
        return self.df

    # --------------------------------------------------
    def test_sql(self, sql_path:str):
        """Create DB for testing purposes"""
        engine = create_engine(f'sqlite:///{sql_path}')
        self.df.to_sql(
            name = 'test',
            con = engine,
            if_exists='replace',
            index=False
        )
        engine.dispose()
        print('Sqlite test done')

    # --------------------------------------------------
    def update_sql_db(self, input_path:str, output_path:str, 
    clean_first:bool=False):
        files = self.get_list_of_files(input_path)
        if clean_first:
            self.delete_all_rows(output_path)
        for file in files:
            self.from_external_report(file)
            if self._FILTER_COL != '':
                self.to_sql(output_path)
            else:
                self.to_sql(output_path, True)