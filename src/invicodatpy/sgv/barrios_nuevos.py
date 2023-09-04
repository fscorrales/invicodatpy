#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write Gestion Viviendas's 
        Informe Barrios Nuevos report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.sgv_model import SGVModel
from ..utils.rpw_utils import RPWUtils


class BarriosNuevos(RPWUtils):
    """Read, process and write Gestion Viviendas's Informe Barrios Nuevos report"""
    _REPORT_TITLE = 'NOMINA DE BARRIOS NUEVOS INCORPORADOS EN EL EJERCICIO'
    _TABLE_NAME = 'barrios_nuevos'
    _INDEX_COL = 'id'
    _FILTER_COL = ['ejercicio']
    _SQL_MODEL = SGVModel

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df['0'].iloc[0][:53]
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
        df['ejercicio'] = df.iloc[0,0][62:66]
        df = df.iloc[4:-6,[0, 2, 6, 8, 9, 13, 14]]
        df.rename({
            '0': 'cod_barrio', 
            '2': 'barrio',
            '6': 'localidad',
            '8': 'q_entregadas',
            '9': 'importe_total',
            '13': 'importe_promedio',
            }, axis='columns', inplace=True)
        df['barrio'] = df['barrio'].str.strip()
        df['importe_total'] = df['importe_total'].astype(float)
        df['importe_promedio'] = df['importe_promedio'].astype(float)
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df = df[cols]

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write GV's Informe Barrios Nuevos report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xlsx_file",
        default='Informe Barrios Nuevos.xlsx',
        type=str,
        help = "SGV' Informe Barrios Nuevos.xlsx report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgv = BarriosNuevos()
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
    # python -m invicodatpy.sgv.barrios_nuevos