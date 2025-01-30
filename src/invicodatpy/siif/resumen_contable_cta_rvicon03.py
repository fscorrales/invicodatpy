#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rvicon03 report
"""

__all__ = ['ResumenContableCtaRvicon03']

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from ..models.siif_model import SIIFModel
from .connect_siif import ConnectSIIF, ReportCategory


@dataclass
class ResumenContableCtaRvicon03(ConnectSIIF):
    """
    Read, process and write SIIF's rvicon03 report
    :param siif_connection must be initialized first in order to download from SIIF
    """
    _REPORT_TITLE:str = field(init=False, repr=False, default='rvicon03')
    _TABLE_NAME:str = field(init=False, repr=False, default='resumen_contable_cta_rvicon03')
    _INDEX_COL:str = field(init=False, repr=False, default='id')
    _FILTER_COL:str = field(init=False, repr=False, default='ejercicio')
    _SQL_MODEL:SIIFModel = field(init=False, repr=False, default=SIIFModel)


    # --------------------------------------------------
    def download_report(
        self, dir_path:str, ejercicios:list = str(dt.datetime.now().year)
    ):
        try:
            self.set_download_path(dir_path)
            self.select_report_module(ReportCategory.Contabilidad)
            self.select_specific_report_by_id('2079')

            # Getting DOM elements
            input_ejercicio = self.get_dom_element(
                    "//input[@id='pt1:txtAnioEjercicio::content']", wait=True
                )
            btn_get_reporte = self.get_dom_element(
                "//*[@id='pt1:btnEjecutarReporte']"
            )
            btn_xls = self.get_dom_element(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            btn_xls.click()

            # Form submit
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                input_ejercicio.clear()
                input_ejercicio.send_keys(ejercicio)
                btn_get_reporte.click()

                # Download and rename xls
                self.rename_report(dir_path, 'rvicon03.xls', ejercicio + '-rvicon03.xls')
                self.download_file_procedure()
            time.sleep(1)

            # Going back to reports list
            self.go_back_to_reports_list()

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.disconnect()

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['17'].iloc[7]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass
        return self.df

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read xls file"""
        df = self.df
        df['ejercicio'] = df.iloc[3,2][-4:]
        df = df.tail(-18)
        df = df.loc[:,[
            'ejercicio', '2', '6', '7', '8', 
            '10', '11', '12', '13', '15'
        ]]
        df = df.replace(to_replace='', value=None)
        df = df.dropna(subset=['2'])
        df = df.rename(columns={
            '2':'nivel_descripcion', 
            '6':'saldo_inicial', 
            '7':'debe', 
            '8':'haber', 
            '10':'ajuste_debe', 
            '11':'ajuste_haber', 
            '12':'fondos_debe', 
            '13':'fondos_haber', 
            '15':'saldo_final'
        })
        df['nivel'] = np.where(
            df['saldo_inicial'].isnull(),
            df['nivel_descripcion'].str[0:4],
            None
        )
        df['nivel'] = df['nivel'].ffill()
        df['nivel_desc'] = np.where(
            df['saldo_inicial'].isnull(),
            df['nivel_descripcion'].str[8:],
            None
        )
        df['nivel_desc'] = df['nivel_desc'].ffill()
        df = df.dropna(subset=['saldo_inicial'])

        df['cta_contable'] = df['nivel_descripcion'].str.split('-', expand=True).iloc[:,:3].agg('-'.join, axis=1)
        df['cta_contable_desc'] = df['nivel_descripcion'].apply(
            lambda x: '-'.join(filter(None, x.split('-')[3:])) if x is not None else None
        )
        df = df.loc[:,[
            'ejercicio', 'nivel', 'nivel_desc', 'cta_contable', 'cta_contable_desc',
            'saldo_inicial', 'debe', 'haber', 'ajuste_debe', 'ajuste_haber', 
            'fondos_debe', 'fondos_haber', 'saldo_final'
        ]]
        to_numeric_cols = [
            'saldo_inicial', 'debe', 'haber', 'ajuste_debe', 'ajuste_haber', 
            'fondos_debe', 'fondos_haber', 'saldo_final'
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric)
        df = df[~df[to_numeric_cols].apply(lambda x: (x == 0).all(), axis=1)]

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rvicon03report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='',
        type=str,
        help = "SIIF' rf610.xls report. Must be in the same folder")

    parser.add_argument('--download', action='store_true')
    parser.add_argument('--no-download', dest='download', action='store_false')
    parser.set_defaults(download=True)

    parser.add_argument(
        '-u', '--username', 
        metavar = 'Username',
        default = '',
        type=str,
        help = "Username to log in SIIF")

    parser.add_argument(
        '-p', '--password', 
        metavar = 'Password',
        default = '',
        type=str,
        help = "Password to log in SIIF")

    parser.add_argument(
        '-e', '--ejercicio', 
        metavar = 'Ejercicio',
        default = '2024',
        type=str,
        help = "Ejercicio to download from SIIF")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))

    if args.download:
        json_path = dir_path + '/siif_credentials.json'
        if args.username != '' and args.password != '':
            ConnectSIIF(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    ConnectSIIF(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        siif = ResumenContableCtaRvicon03()
        siif.go_to_reports()
        siif.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        siif.disconnect()
    else:
        siif = ResumenContableCtaRvicon03()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + '-rvicon03.xls'

    siif.from_external_report(dir_path + '/' + filename)
    # siif.test_sql(dir_path + '/test.sqlite')
    siif.to_sql(dir_path + '/siif.sqlite')
    siif.print_tidyverse()
    siif.from_sql(dir_path + '/siif.sqlite')
    siif.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.resumen_contable_cta_rvicon03 -e 2023 --no-download
