#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write Cuentas Corrientes Mapper
"""

__all__ = ['CtasCtes']

import argparse
import inspect
import os

import pandas as pd

from ..models.sscc_model import SSCCModel
from ..utils.rpw_utils import RPWUtils


class CtasCtes(RPWUtils):
    """Read, process and write Cuentas Corrientes Mapper"""
    _REPORT_TITLE = 'map_to'
    _TABLE_NAME = 'ctas_ctes'
    _INDEX_COL = 'id'
    _FILTER_COL = ''
    _SQL_MODEL = SSCCModel

    # --------------------------------------------------
    def from_external_report(self, path:str) -> pd.DataFrame:
        """"Read from csv SSCC's report"""
        df = self.read_xls(path, header=0)
        read_title = df.columns[0]
        if read_title == self._REPORT_TITLE:
            self.df = df
            #self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        pass

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write Cuentas Corrientes Mapper",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "excel_file",
        default='ctas_ctes.xlsx',
        type=str,
        help = "Ctas Ctes excel file. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    ctas_ctes = CtasCtes()
    ctas_ctes.from_external_report(dir_path + '/' + args.file)
    # ctas_ctes.test_sql(dir_path + '/test.sqlite')
    ctas_ctes.to_sql(dir_path + '/sscc.sqlite', True)
    ctas_ctes.print_tidyverse()
    ctas_ctes.from_sql(dir_path + '/sscc.sqlite')
    ctas_ctes.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sscc.ctas_ctes
