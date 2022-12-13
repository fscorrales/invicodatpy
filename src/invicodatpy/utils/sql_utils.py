#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: SQL methods
"""
from pandas import DataFrame
from sqlalchemy import MetaData, Table, and_, delete, engine


class SQLUtils():
    
    # --------------------------------------------------
    """Delete rows from a table with one or multiple conditions
    :param engine: sqlalchemy engine to connect to
    :param df: pandas DataFrame to look up for unique values
    :param table_filter: Table name from where to delete rows
    :param col_filter: column (str) or multiple columns (str) to
    look for unique values
    """
    def delete_rows_with_df_col(
        self, engine:engine, df:DataFrame,
        table_filter:str, col_filter:str | list
    ):
        connection = engine.connect()
        metadata = MetaData()
        sql_table = Table(table_filter, 
        metadata, autoload=True, autoload_with=engine)
        
        if isinstance(col_filter, list):
            where_lst = []
            for i in col_filter:
                unique_col = df[i].unique()
                where_lst.append(sql_table.c[i].in_(unique_col))
            where_clause = and_(*where_lst)
        else:
            unique_col = df[col_filter].unique()
            where_clause = sql_table.c[col_filter].in_(unique_col)
        
        u = delete(sql_table).where(where_clause)
        result = connection.execute(u)
        return result

    # --------------------------------------------------
    """Delete all rows from a table
    :param engine: sqlalchemy engine to connect to
    :param table_filter: Table name from where to delete rows
    """
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