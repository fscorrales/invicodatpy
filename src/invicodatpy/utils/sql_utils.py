#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: SQL methods
"""
from sqlalchemy import MetaData, Table, delete, engine
from pandas import DataFrame

class SQLUtils():

    def delete_rows_with_df_col(
        self, engine:engine, df:DataFrame,
        table_filter:str, col_filter:str
    ):
        unique_col = df[col_filter].unique()
        metadata = MetaData()
        sql_table = Table(table_filter, 
        metadata, autoload=True, autoload_with=engine)
        connection = engine.connect()
        u = delete(sql_table).where(
            sql_table.c[col_filter].in_(unique_col))
        result = connection.execute(u)
        return result

    def delete_all_rows(
        self, engine:engine, table_filter:str
    ):
        metadata = MetaData()
        sql_table = Table(table_filter, 
        metadata, autoload=True, autoload_with=engine)
        connection = engine.connect()
        u = delete(sql_table)
        result = connection.execute(u)
        return result