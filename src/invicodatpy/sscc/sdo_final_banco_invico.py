#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SSCC's 'Informe de Saldos de Cuentas' report
"""

import argparse
import inspect
import os

import pandas as pd
from ..models.sscc_model import SSCCModel
from ..utils.rpw_utils import RPWUtils

class SdoFinalBancoINVICO(RPWUtils):
    """Read, process and write SSCC's 'Informe de Saldos de Cuentas' report"""
    _REPORT_TITLE = 'Informe de Saldos de Cuentas'
    _TABLE_NAME = 'sdo_final_banco_invico'
    _INDEX_COL = 'id'
    _FILTER_COL = ['ejercicio', 'cta_cte']
    _SQL_MODEL = SSCCModel

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
        df = df.loc[:, ['5', '11', '12', '13', '14']]
        df.columns = ['ejercicio', 'cta_cte', 'desc_cta_cte', 
        'desc_banco', 'saldo']
        df['ejercicio'] =  df['ejercicio'].str[-4:]
        df['saldo'] = df['saldo'].str.replace('.', '', regex=False)
        df['saldo'] = df['saldo'].str.replace(',', '.', regex=False)
        df['saldo'] = df['saldo'].astype(float)

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SSCC's 'Informe " +
        "de Saldos de Cuentas' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='2022 - saldos_sscc.csv',
        type=str,
        help = "SSCC' Informe de Saldos de Cuentas.csv report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sscc_banco_invico = SdoFinalBancoINVICO()
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
    # python -m invicodatpy.sscc.sdo_final_banco_invico
