#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write combined utils
"""

from .handling_files import HandlingFiles
from .print_tidyverse import PrintTidyverse
from .sql_utils import SQLUtils


class RPWUtils(SQLUtils, HandlingFiles):
    df = None
    _TABLE_NAME = ''
    _INDEX_COL = ''
    _FILTER_COL = ''
    _SQL_MODEL = None

    def print_tidyverse(self, data = None):
        if data is None:
            data = self.df
        print(PrintTidyverse(data))