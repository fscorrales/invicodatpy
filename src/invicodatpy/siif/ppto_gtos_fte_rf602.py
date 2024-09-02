#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rf602 report
"""

__all__ = ['PptoGtosFteRf602']

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from ..models.siif_model import SIIFModel
from ..utils.rpw_utils import RPWUtils
from .connect_siif import ConnectSIIF


@dataclass
class PptoGtosFteRf602(RPWUtils):
    """
    Read, process and write SIIF's rf602 report
    :param siif_connection must be initialized first in order to download from SIIF
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='DETALLE DE LA EJECUCION PRESUESTARIA'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='ppto_gtos_fte_rf602'
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
    siif:ConnectSIIF = field(
        init=True, repr=False, default=None
    )

    # --------------------------------------------------
    def connect(self):
        self.siif.connect()

    # --------------------------------------------------
    def go_to_reports(self):
        self.siif.go_to_reports()

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, ejercicios:list = str(dt.datetime.now().year)
    ):
        try:
            # Path de salida
            params = {
            'behavior': 'allow',
            'downloadPath': dir_path
            }
            self.siif.driver.execute_cdp_cmd('Page.setDownloadBehavior', params)

            # Seleccionar módulo Gastos
            cmb_modulos = Select(
                self.siif.driver.find_element(By.XPATH, "//select[@id='pt1:socModulo::content']")
            )
            cmb_modulos.select_by_visible_text('SUB - SISTEMA DE CONTROL DE GASTOS')
            time.sleep(1)

            # Select rf602 report
            input_filter = self.siif.driver.find_element(
                By.XPATH, "//input[@id='_afrFilterpt1_afr_pc1_afr_tableReportes_afr_c1::content']"
            )
            input_filter.clear()
            input_filter.send_keys('38', Keys.ENTER)
            btn_siguiente = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:pc1:btnSiguiente']")
            btn_siguiente.click()

            # Llenado de inputs
            self.siif.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[@id='pt1:txtAnioEjercicio::content']")
            ))
            input_ejercicio = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtAnioEjercicio::content']"
                )
            btn_get_reporte = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:btnVerReporte']")
            btn_xls = self.siif.driver.find_element(By.XPATH, "//input[@id='pt1:rbtnXLS::content']")
            btn_xls.click()
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                input_ejercicio.clear()
                input_ejercicio.send_keys(ejercicio)
                btn_get_reporte.click()
                self.siif.rename_report(dir_path, 'rf602.xls', ejercicio + '-rf602.xls')
                self.siif.wait.until(EC.number_of_windows_to_be(3))
                self.siif.driver.switch_to.window(self.siif.driver.window_handles[2])
                self.siif.driver.close()
                self.siif.driver.switch_to.window(self.siif.driver.window_handles[1])
            time.sleep(1)
            btn_volver = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:btnVolver']")
            btn_volver.click()
            time.sleep(1)

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.siif.disconnect()
            self.siif.quit()

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['2'].iloc[5][:-5] 
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
        df['ejercicio'] = df.iloc[5,2][-4:]
        df = df.tail(-16)
        df = df.loc[:,[
            'ejercicio', '2', '3', '6', '7', '8', 
            '9', '10', '13', '14', '15', '16', '18', '20'
        ]]
        df = df.replace(to_replace='', value=None)
        df = df.dropna(subset=['2'])
        df = df.rename(columns={
            '2':'programa', 
            '3':'subprograma', 
            '6':'proyecto', 
            '7':'actividad', 
            '8':'partida', 
            '9':'fuente', 
            '10':'org', 
            '13':'credito_original', 
            '14':'credito_vigente', 
            '15':'comprometido', 
            '16':'ordenado', 
            '18':'saldo', 
            '20':'pendiente'
        })
        df['programa'] = df['programa'].str.zfill(2)
        df['subprograma'] = df['subprograma'].str.zfill(2)
        df['proyecto'] = df['proyecto'].str.zfill(2)
        df['actividad'] = df['actividad'].str.zfill(2)
        df['grupo'] = df['partida'].str[0] + '00'
        df['estructura'] = (
            df['programa'] + '-' + df['subprograma'] + '-' + df['proyecto'] + '-' + 
            df['actividad'] + '-' + df['partida']
        )
        df = df.loc[:,[
            'ejercicio', 'estructura', 'fuente', 
            'programa', 'subprograma', 'proyecto', 
            'actividad', 'grupo', 'partida',
            'org', 'credito_original', 'credito_vigente',
            'comprometido', 'ordenado', 'saldo', 'pendiente'
        ]]
        to_numeric_cols = [
            'credito_original', 'credito_vigente', 
            'comprometido', 'ordenado', 'saldo', 'pendiente'
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric).astype(np.float64) 

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rf602 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='',
        type=str,
        help = "SIIF' rf602.xls report. Must be in the same folder")

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
            siif_connection = ConnectSIIF(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    siif_connection = ConnectSIIF(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        siif_rf602 = PptoGtosFteRf602(siif = siif_connection)
        siif_rf602.connect()
        siif_rf602.go_to_reports()
        siif_rf602.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        siif_connection.disconnect()
    else:
        siif_rf602 = PptoGtosFteRf602()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + '-rf602.xls'

    siif_rf602.from_external_report(dir_path + '/' + filename)
    # siif_rf602.test_sql(dir_path + '/test.sqlite')
    siif_rf602.to_sql(dir_path + '/siif.sqlite')
    siif_rf602.print_tidyverse()
    siif_rf602.from_sql(dir_path + '/siif.sqlite')
    siif_rf602.print_tidyverse()
    # siif_rf602.update_sql_db(dir_path + '/' + filename, dir_path + '/siif.sqlite')

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.ppto_gtos_fte_rf602
    # python -m invicodatpy.siif.ppto_gtos_fte_rf602 --no-download
