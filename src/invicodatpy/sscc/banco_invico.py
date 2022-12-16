#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SSCC's 'Consulta 
General de Movimientos' report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, tidyr,f
from .sscc import SSCC

class BancoINVICO(SSCC):
    """Read, process and write SSCC's 'Consulta General de Movimientos' report"""
    _REPORT_TITLE = 'Consulta General de Movimientos'
    _TABLE_NAME = 'banco_invico'
    _INDEX_COL = 'id'
    _FILTER_COL = 'mes'

    # --------------------------------------------------
    def from_external_report(self, csv_path:str) -> pd.DataFrame:
        """"Read from csv SSCC's report"""
        df = self.read_csv(csv_path)
        read_title = df['1'].iloc[0]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read csv file"""
        df = self.df
        df = df.replace(to_replace='[\r\n]', value='')
        df = df >> \
            dplyr.transmute(
                fecha = f['20'],
                ejercicio = f.fecha.str[-4:],
                mes = f.fecha.str[3:5] + '/' + f.ejercicio,
                cta_cte = f['22'],
                movimiento = base.trimws(f['21']),
                es_cheque = dplyr.case_when(
                    (f.movimiento == "DEBITO") | (f.movimiento == "DEPOSITO"), False,
                    True, True
                ),
                concepto = f['23'],
                beneficiario = f['24'],
                moneda = f['25'],
                libramiento = f['26'],
                imputacion = f['27'],
                importe = base.as_double(
                    base.gsub(',', '', f['28']))
            ) >> \
            tidyr.separate(
                f.imputacion, 
                into = ['cod_imputacion', 'imputacion'], 
                sep= '-' ,remove=True, extra='merge'
            ) >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha,
                f.cta_cte, f.movimiento, f.es_cheque,
                f.beneficiario, f.importe,
                dplyr.everything()
            )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%d/%m/%Y'
        )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SSCC's 'Consulta " +
        "General de Movimientos' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='2022 - Bancos - Consulta General de Movimientos.csv',
        type=str,
        help = "SSCC' Consulta General de Movimientos.csv report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sscc_banco_invico = BancoINVICO()
    sscc_banco_invico.from_external_report(dir_path + '/' + args.file)
    # sscc_banco_invico.test_sql(dir_path + '/test.sqlite')
    sscc_banco_invico.to_sql(dir_path + '/sscc.sqlite')
    sscc_banco_invico.print_tidyverse()
    sscc_banco_invico.from_sql(dir_path + '/sscc.sqlite')
    sscc_banco_invico.print_tidyverse()
    # print(sscc_banco_invico.df.head(10))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sscc.banco_invico
