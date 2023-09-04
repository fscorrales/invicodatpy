#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write Gestion Viviendas's 
        Informe de Saldos por Barrio report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.sgv_model import SGVModel
from ..utils.rpw_utils import RPWUtils


class SaldoBarrio(RPWUtils):
    """Read, process and write Gestion Viviendas's Informe Informe de Saldos por Barrio report"""
    _REPORT_TITLE = 'INFORME DE SALDOS POR BARRIO'
    _TABLE_NAME = 'saldo_barrio'
    _INDEX_COL = 'id'
    _FILTER_COL = ['ejercicio']
    _SQL_MODEL = SGVModel

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df.iloc[0,0][:28]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read xls file"""
        df = self.df
        df['ejercicio'] = df.iloc[0,0][-4:]
        df = df.iloc[5:-1, [0, 1, 2, 3, 5, 6]]
        df.rename({
            '0': 'cod_barrio', 
            '1': 'barrio',
            '2': 'saldo_vencido',
            '3': 'saldo_actual',
            '5': 'localidad',
            }, axis='columns', inplace=True)
        cols = ['saldo_vencido', 'saldo_actual']
        for col in cols:
            df[col] = df[col].astype(float)
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df = df[cols]

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write GV's Informe de Saldos por Barrio report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = 'xlsx_file',
        default='InformeSaldosPorBarrio.xlsx',
        type=str,
        help = "SGV' Informe de Saldos por Barrio.xlsx report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgv = SaldoBarrio()
    sgv.from_external_report(dir_path + '/' + args.file)
    # sgv.test_sql(dir_path + '/test.sqlite')
    sgv.to_sql(dir_path + '/sgv.sqlite')
    sgv.print_tidyverse()
    sgv.from_sql(dir_path + '/sgv.sqlite')
    sgv.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgv.saldo_barrio