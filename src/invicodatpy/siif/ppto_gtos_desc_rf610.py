#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rf610 report
"""

__all__ = ['PptoGtosDescRf610']

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
from .connect_siif import ConnectSIIF

@dataclass
class PptoGtosDescRf610(ConnectSIIF):
    """
    Read, process and write SIIF's rf610 report
    :param siif_connection must be initialized first in order to download from SIIF
    """
    _REPORT_TITLE:str = field(init=False, repr=False, default='LISTADO DE EJECUCION DE GASTOS POR PARTIDA')
    _TABLE_NAME:str = field(init=False, repr=False, default='ppto_gtos_desc_rf610')
    _INDEX_COL:str = field(init=False, repr=False, default='id')
    _FILTER_COL:str = field(init=False, repr=False, default='ejercicio')
    _SQL_MODEL:SIIFModel = field(init=False, repr=False, default=SIIFModel)

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, ejercicios:list = str(dt.datetime.now().year)
    ):
        try:
            self.set_download_path(dir_path)
            self.select_report_module('SUB - SISTEMA DE CONTROL DE GASTOS')
            self.select_specific_report_by_id('7')
            
            # Getting DOM elements
            input_ejercicio = self.get_dom_element(
                "//input[@id='pt1:txtAnioEjercicio::content']", wait=True
            )
            btn_get_reporte = self.get_dom_element(
                "//div[@id='pt1:btnVerReporte']"
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
                input_ejercicio.clear()
                input_ejercicio.send_keys(ejercicio)
                btn_get_reporte.click()

                # Download and rename xls
                self.rename_report(dir_path, 'rf610.xls', ejercicio + '-rf610.xls')
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
        read_title = df['32'].iloc[2] + ' ' + df['32'].iloc[4]
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
        df = self.df.replace(to_replace='', value=None)
        df['ejercicio'] = df.iloc[9,33][-4:]
        df = df.rename(columns={
            '5':'programa',
            '9':'subprograma',
            '14':'proyecto',
            '17':'actividad',
            '20':'grupo',
            '21':'partida',
            '24':'desc_part',
            '38':'credito_original',
            '44':'credito_vigente',
            '49':'comprometido',
            '55':'ordenado',
            '60':'saldo'
        })
        df = df.tail(-30)
        df = df.loc[:, [
            'ejercicio', 'programa', 'subprograma', 'proyecto', 
            'actividad', 'grupo', 'partida', 'desc_part', 
            'credito_original', 'credito_vigente', 'comprometido', 'ordenado', 'saldo'
        ]]
        df['programa'] = df['programa'].fillna(method='ffill')
        df['subprograma'] = df['subprograma'].fillna(method='ffill')
        df['proyecto'] = df['proyecto'].fillna(method='ffill')
        df['actividad'] = df['actividad'].fillna(method='ffill')
        df['grupo'] = df['grupo'].fillna(method='ffill')
        df['partida'] = df['partida'].fillna(method='ffill')
        df['desc_part'] = df['desc_part'].fillna(method='ffill')
        df = df.dropna(subset=['credito_original'])
        df[["programa", "desc_prog"]] = df["programa"].str.split(n=1, expand=True)
        df[["subprograma", "desc_subprog"]] = df["subprograma"].str.split(n=1, expand=True)
        df[["proyecto", "desc_proy"]] = df["proyecto"].str.split(n=1, expand=True)
        df[["actividad", "desc_act"]] = df["actividad"].str.split(n=1, expand=True)
        df[["grupo", "desc_gpo"]] = df["grupo"].str.split(n=1, expand=True)
        df['programa'] = df['programa'].str.zfill(2)
        df['subprograma'] = df['subprograma'].str.zfill(2)
        df['proyecto'] = df['proyecto'].str.zfill(2)
        df['actividad'] = df['actividad'].str.zfill(2)
        df['desc_prog'] = df['desc_prog'].str.strip()
        df['desc_subprog'] = df['desc_subprog'].str.strip()
        df['desc_proy'] = df['desc_proy'].str.strip()
        df['desc_act'] = df['desc_act'].str.strip()
        df['desc_gpo'] = df['desc_gpo'].str.strip()
        df['desc_part'] = df['desc_part'].str.strip()
        df['estructura'] = (
            df['programa'] + '-' + df['subprograma'] + '-' + df['proyecto'] + '-' + 
            df['actividad'] + '-' + df['partida']
        )
        to_numeric_cols = [
            'credito_original', 'credito_vigente', 
            'comprometido', 'ordenado', 'saldo',
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric).astype(np.float64) 

        first_cols = [
            'ejercicio', 'estructura', 'programa', 'desc_prog', 
            'subprograma', 'desc_subprog', 'proyecto', 'desc_proy', 
            'actividad', 'desc_act', 'grupo', 'desc_gpo',
            'partida', 'desc_part'
        ]
        df = df.loc[:, first_cols].join(df.drop(first_cols, axis=1))

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rf610 report",
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
        default = '2022',
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

    try:
        if args.download:
            if args.username != '' and args.password != '':
                ConnectSIIF.connect(args.username, args.password)
            else:
                json_path = dir_path + '/siif_credentials.json'
                if os.path.isfile(json_path):
                    with open(json_path) as json_file:
                        data_json = json.load(json_file)
                        ConnectSIIF.connect(
                            data_json['username'], data_json['password']
                        )
                    json_file.close()
                # ConnectSIIF.connect(
                #     username = SIIF_USERNAME, password = SIIF_PASSWORD
                # )
            siif = PptoGtosDescRf610()
            siif.go_to_reports()
            siif.download_report(
                dir_path, ejercicios=args.ejercicio
            )
            siif.disconnect()
        else:
            siif = PptoGtosDescRf610()

        if args.file != '':
            filename = args.file
        else:
            filename = args.ejercicio + '-rf610.xls'

        siif.from_external_report(dir_path + '/' + filename)
        # siif.test_sql(dir_path + '/test.sqlite')
        siif.to_sql(dir_path + '/siif.sqlite')
        siif.print_tidyverse()
        siif.from_sql(dir_path + '/siif.sqlite')
        siif.print_tidyverse()
        # dir = '/home/kanou/IT/R Apps/R Gestion INVICO/invicoDB/Base de Datos/Reportes SIIF/Ejecucion Presupuestaria con Descripcion (rf610)'
        # siif.update_sql_db(dir, dir_path + '/siif.sqlite', True)
    except Exception as e:
        print(f"Ocurrio un error: {e}, {type(e)}")
        try:
            siif.disconnect()
        except Exception as e:
            print(f"Ocurrio un error: {e}, {type(e)}")


# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.ppto_gtos_desc_rf610 -e 2024 --no-download
