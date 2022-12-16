#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: SQL methods
"""
import pandas as pd
from sqlalchemy import MetaData, Table, and_, create_engine, delete, engine


class SQLUtils():
    "Some generals methods"
    df = None
    _TABLE_NAME = ''
    _INDEX_COL = ''
    _FILTER_COL = ''
    _SQL_MODEL = None
    
    # --------------------------------------------------
    """Delete rows from a table with one or multiple conditions
    :param engine: sqlalchemy engine to connect to
    :param df: pandas DataFrame to look up for unique values
    :param table_filter: Table name from where to delete rows
    :param col_filter: column (str) or multiple columns (str) to
    look for unique values
    """
    def delete_rows_with_df_col(self):
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
    def delete_all_rows(self):
        """Delete all rows from a table
        :param engine: sqlalchemy engine to connect to
        :param table_filter: Table name from where to delete rows
        """
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
        self.engine = self._SQL_MODEL(sql_path).engine        
        if replace:
            self.delete_all_rows()            
        else:
            self.delete_rows_with_df_col()
        
        self.df.to_sql(
            name = self._TABLE_NAME,
            con = self.engine,
            if_exists = 'append',
            index=False
        )
        self.engine.dispose()

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