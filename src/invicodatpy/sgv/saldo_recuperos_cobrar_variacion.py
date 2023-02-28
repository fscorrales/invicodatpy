#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write Gestion Viviendas's 
        Informe Variación de Saldos Recuperos a Cobrar report
"""

import argparse
import inspect
import os

import pandas as pd
import numpy as np
from datar import base, dplyr, f, tidyr

from ..models.sgv_model import SGVModel
from ..utils.rpw_utils import RPWUtils


class SaldoRecuperosCobrarVariacion(RPWUtils):
    """Read, process and write Gestion Viviendas's Informe Variación de Saldos Recuperos a Cobrar report"""
    _REPORT_TITLE = 'VARIACIÓN DE SALDOS DE RECUPEROS A COBRAR'
    _TABLE_NAME = 'saldo_recuperos_cobrar_variacion'
    _INDEX_COL = 'id'
    _FILTER_COL = ['ejercicio']
    _SQL_MODEL = SGVModel

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df.iloc[0,0][:41]
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
        df = df.iloc[:, [0, 5, 6]]
        df.replace('', np.NaN, inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        df['ejercicio'] = df.iloc[0,0][-4:]
        df.columns = ['concepto', 'importe_borrar', 'importe', 'ejercicio']
        df.concepto.fillna(value=None, method="ffill", inplace=True)
        df.importe.fillna(value=df["importe_borrar"], inplace=True)
        df.drop("importe_borrar", axis = 1,inplace=True)
        df.dropna(axis=0, how='any', inplace=True)        
        cols = ['importe']
        for col in cols:
            df[col] = df[col].astype(float)
        df.loc[df['concepto'] == 'COBRANZA:', 'importe'] = df['importe'] * (-1)
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df = df[cols]

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write GV's Informe Variación de Saldos Recuperos a Cobrar report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = 'xlsx_file',
        default='InformeVariacionSaldosRecuperosCobrar.xlsx',
        type=str,
        help = "SGV' Informe Variación de Saldos Recuperos a Cobrar.xlsx report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgv = SaldoRecuperosCobrarVariacion()
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
    # python -m invicodatpy.sgv.saldo_recuperos_cobrar_variacion