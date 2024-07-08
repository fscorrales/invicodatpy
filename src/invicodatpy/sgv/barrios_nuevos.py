#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write Gestion Viviendas's 
        Informe Barrios Nuevos report
"""

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC

from ..models.sgv_model import SGVModel
from ..utils.rpw_utils import RPWUtils
from .connect_sgv import ConnectSGV

@dataclass
class BarriosNuevos(RPWUtils):
    """Read, process and write Gestion Viviendas's Informe Barrios Nuevos report"""
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='NOMINA DE BARRIOS NUEVOS INCORPORADOS EN EL EJERCICIO'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='barrios_nuevos'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default='ejercicio'
    )
    _SQL_MODEL:SGVModel = field(
        init=False, repr=False, default=SGVModel
    )
    sgv:ConnectSGV = field(
        init=True, repr=False, default=None
    )

   # --------------------------------------------------
    def download_report(
        self, dir_path:str, ejercicios:list = str(dt.datetime.now().year)
    ):
        """
        Download the 'Informe de Evolución Saldos Por Motivos' report from Sistema Recuperos.

        """
        try:
            # Path de salida
            params = {
            'behavior': 'allow',
            'downloadPath': dir_path
            }
            self.sgv.driver.execute_cdp_cmd('Page.setDownloadBehavior', params)

            # Abrir una nueva pestaña
            self.sgv.driver.execute_script("window.open('about:blank', 'new_tab')")

            # Cambiar el enfoque a la nueva pestaña
            self.sgv.driver.switch_to.window(self.sgv.driver.window_handles[1])

            # Navegar a una dirección web específica en la nueva pestaña
            self.sgv.driver.get('https://gv.invico.gov.ar/App/Recupero/Informes/InformeBarriosNuevosIncorporados.aspx')

            time.sleep(1)

            # Llenado de inputs
            xpath_input_gral = "//table[@class='tablaFiltros']//input"
            xpath_ejercicio = "//input[@id='ctl00_ContentPlacePrincipal_ucBarriosNuevosIncorporadosPorAnio_txtAño_TextBox1']"
            self.sgv.wait.until(EC.presence_of_element_located(
                (By.XPATH, xpath_ejercicio)
            ))

            # Bajando Reportes
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio < (dt.datetime.now().year + 1):
                    # Ejercicio
                    input_ejercicio= self.sgv.driver.find_element(
                        By.XPATH, xpath_ejercicio
                    )
                    input_ejercicio.clear()
                    input_ejercicio.send_keys(ejercicio, Keys.ENTER)
                    self.sgv.wait.until(EC.presence_of_element_located(
                        (By.XPATH, "/html/body/form/div[3]/table/tbody/tr/td[1]/div/table[2]/tbody/tr/td[3]/div[2]/span/div/table/tbody/tr[4]/td[3]/div/div[1]/div/table/tbody/tr/td/table/tbody/tr[1]/td/table/tbody/tr/td/table")
                    ))
                    btn_export = self.sgv.driver.find_element(By.XPATH, "//a[@id='ctl00_ContentPlacePrincipal_ucBarriosNuevosIncorporadosPorAnio_rpInforme_ctl05_ctl04_ctl00_ButtonLink']")
                    btn_export.click()
                    time.sleep(1)
                    btn_to_excel = self.sgv.driver.find_element(By.XPATH, "//a[@title='Excel']")
                    btn_to_excel.click()
                    self.sgv.wait.until(EC.number_of_windows_to_be(2))
                    self.sgv.rename_report(
                        dir_path, 
                        'Informe Barrios Nuevos.xlsx', 
                        ejercicio + '-InformeBarriosNuevos.xlsx'
                    )
                    time.sleep(1)
                    self.sgv.driver.refresh()
            self.sgv.driver.close()
            self.sgv.driver.switch_to.window(self.sgv.driver.window_handles[0])
            time.sleep(1)

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.sgv.driver.switch_to.window(self.sgv.driver.window_handles[0])
            self.sgv.disconnect()

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df['0'].iloc[0][:53]
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
        df['ejercicio'] = df.iloc[0,0][62:66]
        df = df.iloc[4:-6,[0, 2, 6, 8, 9, 13, 14]]
        df.rename({
            '0': 'cod_barrio', 
            '2': 'barrio',
            '6': 'localidad',
            '8': 'q_entregadas',
            '9': 'importe_total',
            '13': 'importe_promedio',
            }, axis='columns', inplace=True)
        df['cod_barrio'] = df['cod_barrio'].astype(int)
        df['importe_total'] = df['importe_total'].astype(float)
        df['importe_promedio'] = df['importe_promedio'].astype(float)
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df = df[cols]

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write GV's Informe Barrios Nuevos report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--download', action='store_true')
    parser.add_argument('--no-download', dest='download', action='store_false')
    parser.set_defaults(download=True)

    parser.add_argument(
        '-u', '--username', 
        metavar = 'Username',
        default = '',
        type=str,
        help = "Username to log in Gestión Vivienda")

    parser.add_argument(
        '-p', '--password', 
        metavar = 'Password',
        default = '',
        type=str,
        help = "Password to log in Gestión Vivienda")

    parser.add_argument(
        '-e', '--ejercicio', 
        nargs='*',
        metavar = 'Ejercicio',
        default = '2023',
        type=str,
        help = "Ejercicio to download from Gestión Vivienda")

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
        json_path = dir_path + '/credentials.json'
        if args.username != '' and args.password != '':
            sgv_connection = ConnectSGV(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    sgv_connection = ConnectSGV(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        sgv = BarriosNuevos(sgv = sgv_connection)
        sgv.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        sgv_connection.disconnect()
        sgv_connection.remove_html_files(dir_path)
    else:
        sgv = BarriosNuevos()

    if not isinstance(args.ejercicio, list):
        file = [args.ejercicio]
    else:
        file = args.ejercicio

    file = file[0] + '-InformeBarriosNuevos.xlsx'

    sgv.from_external_report(dir_path + '/' + file)
    # sgv.test_sql(dir_path + '/test.sqlite')
    sgv.to_sql(dir_path + '/sgv.sqlite')
    sgv.print_tidyverse()
    sgv.from_sql(dir_path + '/sgv.sqlite')
    sgv.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgv.barrios_nuevos