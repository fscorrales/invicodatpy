#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write Gestion Viviendas's 
        Informe Motivo Actualización Semestral report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.sgv_model import SGVModel
from ..utils.rpw_utils import RPWUtils


class SaldoMotivoActualizacionSemestral(RPWUtils):
    """Read, process and write Gestion Viviendas's Informe Motivo Actualización Semestral report"""
    _REPORT_TITLE = 'INFORME EVOLUCION SALDOS POR MOTIVOS'
    _TABLE_NAME = 'saldo_motivo_individual'
    _INDEX_COL = 'id'
    _FILTER_COL = ['ejercicio', 'motivo']
    _SQL_MODEL = SGVModel

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df.iloc[0,0][:36]
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
        df['ejercicio'] = df.iloc[0,0][-5:][0:4]
        df['cod_motivo'] = '0'
        df['motivo'] = 'actualizacion semestral'
        df = df.iloc[4:-1, [1, 2, 3, 4, 5, 6]]
        df.rename({
            '1': 'cod_barrio', 
            '2': 'barrio',
            '3': 'importe',
            }, axis='columns', inplace=True)
        cols = ['importe']
        for col in cols:
            df[col] = df[col].astype(float)
        cols = df.columns.tolist()
        cols = cols[-2:] + cols[:-2]
        df = df[cols]

        self.df = df
        return self.df

    def from_sql(self, sql_path: str, table_name: str = None) -> pd.DataFrame:
        df = super().from_sql(sql_path, table_name)
        df = df.loc[df['cod_motivo'] == '0']
        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write GV's Informe Motivo Actualización Semestral report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = 'xlsx_file',
        default='InformeMotivoActualizacionSemestral.xlsx',
        type=str,
        help = "SGV' Informe Actualizacion Semestral.xlsx report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgv = SaldoMotivoActualizacionSemestral()
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
    # python -m invicodatpy.sgv.saldo_motivo_actualizacion_semestral