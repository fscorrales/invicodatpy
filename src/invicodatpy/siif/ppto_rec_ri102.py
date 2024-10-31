#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rci02 report
"""

__all__ = ['PptoRecRi102']

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field

import pandas as pd

from ..models.siif_model import SIIFModel
from .connect_siif import ConnectSIIF


@dataclass
class PptoRecRi102(ConnectSIIF):
    """
    Read, process and write SIIF's rci02 report
    :param siif_connection must be initialized first in order to download from SIIF
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='ri102'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='ppto_rec_ri102'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default='ejercicio'
    )
    _SQL_MODEL:SIIFModel = field(
        init=False, repr=False, default=SIIFModel
    )

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, ejercicios:list = str(dt.datetime.now().year)
    ):
        try:
            self.set_download_path(dir_path)
            self.select_report_module('SUB - SISTEMA DE CONTROL de RECURSOS')
            self.select_specific_report_by_id('28')
            
            # Getting DOM elements
            input_ejercicio = self.get_dom_element(
                    "//input[@id='pt1:txtAnioEjercicio::content']", wait=True
                )
            btn_get_reporte = self.get_dom_element(
                "//div[@id='pt1:btnEjecutarReporte']"
            )
            btn_xls = self.get_dom_element(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            btn_xls.click()
            btn_volver = self.get_dom_element(
                "//div[@id='pt1:btnVolver']"
            )

            # Form submit
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                    # Ejercicio
                    input_ejercicio.clear()
                    input_ejercicio.send_keys(ejercicio)
                    btn_get_reporte.click()

                    # Download and rename xls
                    self.rename_report(dir_path, 'ri102.xls', ejercicio + '-ri102.xls')
                    self.download_file_procedure()
            time.sleep(1)
            
            # Going back to reports list
            btn_volver.click()
            time.sleep(1)

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.disconnect()

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['27'].iloc[9]
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
        df['ejercicio'] = df.iloc[5,17]
        df['tipo'] = df['2'].str[0:2] + '000'
        df['clase'] = df['2'].str[0:3] + '00'
        df = df.replace(to_replace='', value=None)
        df = df.loc[:,[
            'ejercicio', 'tipo', 'clase', '2', '4',
            '11', '12', '14', '15', '19', '22', '25'
        ]]
        df = df.dropna(subset=['4'])
        df = df.rename(columns={
            '2':'cod_rec', 
            '4':'desc_rec', 
            '11':'fuente', 
            '12':'org_fin', 
            '14':'ppto_inicial', 
            '15':'ppto_modif', 
            '19':'ppto_vigente', 
            '22':'ingreso', 
            '25':'saldo'
        })     

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rci02 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='',
        type=str,
        help = "SIIF' rci02.xls report. Must be in the same folder")

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
        default = '2023',
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
        siif = PptoRecRi102()
        siif.go_to_reports()
        siif.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        siif.disconnect()
    else:
        siif = PptoRecRi102()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + '-ri102.xls'

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
    # python -m invicodatpy.siif.ppto_rec_ri102 --no-download