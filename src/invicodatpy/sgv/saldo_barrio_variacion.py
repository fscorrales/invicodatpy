#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write Gestion Viviendas's 
        Informe Evolución de Saldos por Barrio report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.sgv_model import SGVModel
from ..utils.rpw_utils import RPWUtils


class SaldoBarrioVariacion(RPWUtils):
    """Read, process and write Gestion Viviendas's Informe Evolución de Saldos por Barrio Nuevos report"""
    _REPORT_TITLE = 'EVOLUCIÓN DE SALDOS POR BARRIO'
    _TABLE_NAME = 'saldo_barrio_variacion'
    _INDEX_COL = 'id'
    _FILTER_COL = ['ejercicio']
    _SQL_MODEL = SGVModel

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df.iloc[1,0][:30]
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
        df['ejercicio'] = df.iloc[1,0][-5:][0:4]
        df = df.iloc[6:-1, [0, 1, 2, 3, 4, 6, 7]]
        df.rename({
            '0': 'cod_barrio', 
            '1': 'barrio',
            '2': 'saldo_inicial',
            '3': 'amortizacion',
            '4': 'cambios',
            '6': 'saldo_final',
            }, axis='columns', inplace=True)
        cols = ['saldo_inicial', 'amortizacion', 'cambios', 'saldo_final']
        for col in cols:
            df[col] = df[col].astype(float)
        df['amortizacion'] = df['amortizacion'] * (-1)
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df = df[cols]

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write GV's Informe Evolución de Saldos por Barrio report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = 'xlsx_file',
        default='InformeEvolucionDeSaldosPorBarrio.xlsx',
        type=str,
        help = "SGV' Informe Evolucion De Saldos Por Barrio.xlsx report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgv = SaldoBarrioVariacion()
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
    # python -m invicodatpy.sgv.saldo_barrio_variacion