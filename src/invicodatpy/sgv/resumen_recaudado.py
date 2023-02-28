#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write Gestion Viviendas's 
        Informe Resumen Recaudado report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.sgv_model import SGVModel
from ..utils.rpw_utils import RPWUtils


class ResumenRecaudado(RPWUtils):
    """Read, process and write Gestion Viviendas's Informe Resumen Recaudado report"""
    _REPORT_TITLE = 'Resumen Recaudado'
    _TABLE_NAME = 'resumen_recaudado'
    _INDEX_COL = 'id'
    _FILTER_COL = ['ejercicio']
    _SQL_MODEL = SGVModel

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df.iloc[3,6][:17]
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
        df['ejercicio'] = df.iloc[3,6][41:45]
        df = df.iloc[8:-1,[2, 3, 4, 5, 7, 8, 9, 10, 11, 13, 14, 15, 16]]
        df.rename({
            '2': 'mes', 
            '3': 'amortizacion',
            '4': 'int_financiero',
            '5': 'int_mora',
            '7': 'gtos_adm',
            '8': 'seg_incendio',
            '9': 'seg_vida',
            '10': 'subsidio',
            '11': 'pago_amigable',
            '13': 'escritura',
            '14': 'pend_acreditacion',
            '15': 'recaudado_total',
            }, axis='columns', inplace=True)
        cols = ['amortizacion', 'int_financiero', 'int_mora', 'gtos_adm', 'seg_incendio', 
        'seg_vida', 'subsidio', 'pago_amigable', 'escritura', 'pend_acreditacion','recaudado_total']
        for col in cols:
            df[col] = df[col].astype(float)
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df = df[cols]
        df['mes'] = df['mes'].str.zfill(2) + '/' + df['ejercicio']

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write GV's Informe Resumen Recaudado report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = 'xlsx_file',
        default='InformeResumenRecaudado.xlsx',
        type=str,
        help = "SGV' Informe Resumen Recaudado.xlsx report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgv = ResumenRecaudado()
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
    # python -m invicodatpy.sgv.resumen_recaudado