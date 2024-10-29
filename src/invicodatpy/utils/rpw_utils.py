#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write combined utils
"""


__all__ = ['RPWUtils']


from .handling_files import read_csv, read_xls, get_list_of_files
from .print_tidyverse import PrintTidyverse
import pandas as pd
from .sql_utils import SQLUtils


class RPWUtils(SQLUtils):

    def read_csv(self, PATH:str, names=None, header=None) -> pd.DataFrame:
        return read_csv(PATH=PATH, names=names, header=header)


    def read_xls(self, PATH:str, header:int = None) -> pd.DataFrame:
        return read_xls(PATH=PATH, header=header)


    def get_list_of_files(self, path:str) -> list:
        return get_list_of_files(path=path)


    def print_tidyverse(self, data = None):
        if data is None:
            data = self.df
        print(PrintTidyverse(data))