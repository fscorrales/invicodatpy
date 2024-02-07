#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write Gestion Viviendas's 
        Informe Motivo Entrega Viviendas report
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

def default_filter_col():
    return ['ejercicio', 'motivo']

@dataclass
class SaldoMotivoPorBarrio(RPWUtils):
    """Read, process and write Gestion Viviendas's Informe Motivo Entrega Viviendas report"""
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='INFORME EVOLUCION SALDOS POR MOTIVOS'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='saldo_motivo_por_barrio'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default = 'ejercicio'
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
            self.sgv.driver.get('https://gv.invico.gov.ar/App/Recupero/Informes/InformeEvolucionDeSaldosPorMotivos.aspx')

            time.sleep(1)

            # Llenado de inputs
            xpath_input_gral = "//table[@class='tablaFiltros']//input"
            xpath_ejercicio = "//input[@id='ctl00_ContentPlacePrincipal_ucInformeEvolucionDeSaldosPorMotivos_txtAño_TextBox1']"
            self.sgv.wait.until(EC.presence_of_element_located(
                (By.XPATH, xpath_ejercicio)
            ))

            # Generando Dataframe de Motivos
            df_motivos = pd.DataFrame(columns=[
                'cod_motivo', 'motivo', 'cod_barrio', 'barrio', 'importe', 'ejercicio'
            ])

            # Bajando Reportes
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio < (dt.datetime.now().year + 1):
                    # Ejercicio
                    input_ejercicio = self.sgv.driver.find_element(
                        By.XPATH, xpath_ejercicio
                    )
                    input_ejercicio.clear()
                    input_ejercicio.send_keys(ejercicio, Keys.ENTER)
                    table_motivos = "/html/body/form/div[3]/table/tbody/tr/td[1]/div/table[2]/tbody/tr/td[3]/div[2]/span/div/table/tbody/tr[4]/td[3]/div/div[1]/div/table/tbody/tr/td/table/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr[4]/td[3]/table"
                    self.sgv.wait.until(EC.presence_of_element_located(
                        (By.XPATH, table_motivos)
                    ))
                    cod_motivos = self.sgv.driver.find_elements(
                        By.XPATH, table_motivos + "//a"
                    )
                    desc_motivos = self.sgv.driver.find_elements(
                        By.XPATH, table_motivos + "//td[position()=3]"
                    )
                    for i in range(len(cod_motivos)):
                        cod_motivos = self.sgv.driver.find_elements(
                            By.XPATH, table_motivos + "//a"
                        )
                        desc_motivos = self.sgv.driver.find_elements(
                            By.XPATH, table_motivos + "//td[position()=3]"
                        )
                        cod_motivo_str = cod_motivos[i].text
                        desc_motivo_str = desc_motivos[i+2].text
                        # print(f"{ejercicio} - {cod_motivo_str} - {desc_motivo_str}")
                        cod_motivos[i].click()
                        time.sleep(1)
                        self.sgv.wait.until(EC.presence_of_element_located(
                            (By.XPATH, table_motivos)
                        ))
                        time.sleep(1)
                        btn_export = self.sgv.driver.find_element(By.XPATH, "//a[@id='ctl00_ContentPlacePrincipal_ucInformeEvolucionDeSaldosPorMotivos_rpInformeEvoSaldosPorMotivos_ctl05_ctl04_ctl00_ButtonLink']")
                        btn_export.click()
                        time.sleep(1)
                        btn_to_excel = self.sgv.driver.find_element(By.XPATH, "//a[@title='Excel']")
                        btn_to_excel.click()
                        self.sgv.wait.until(EC.number_of_windows_to_be(2))
                        file_name = ejercicio + '-Provisorio '+ cod_motivo_str +'.xlsx'
                        self.sgv.rename_report(
                            dir_path, 
                            'RecuperosInformeEvoSaldosPorMotivoPorBarrio.xlsx', 
                            file_name
                        )
                        self.from_external_report_temp(dir_path + '/' + file_name)
                        self.transform_df(
                            ejercicio=ejercicio,
                            cod_motivo=cod_motivo_str, 
                            motivo=desc_motivo_str,
                        )
                        df_motivos = pd.concat([df_motivos, self.df])
                        os.remove(dir_path + '/' + file_name)
                        input_ejercicio = self.sgv.driver.find_element(
                            By.XPATH, xpath_ejercicio
                        )
                        input_ejercicio.send_keys(Keys.ENTER)
                        self.sgv.wait.until(EC.presence_of_element_located(
                            (By.XPATH, table_motivos)
                        ))
                    time.sleep(1)
                file_name = dir_path + '/' + ejercicio + '-RecuperosInformeEvoSaldosPorMotivoPorBarrio.xlsx'
                if os.path.isfile(file_name):
                    os.remove(file_name)
                    time.sleep(2)
                with pd.ExcelWriter(file_name) as writer:
                    df_motivos.to_excel(writer, sheet_name='MotivosPorBarrio', index=False)
                df_motivos = df_motivos.drop(df_motivos.index)
            self.sgv.driver.close()
            self.sgv.driver.switch_to.window(self.sgv.driver.window_handles[0])
            time.sleep(1)

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.sgv.driver.switch_to.window(self.sgv.driver.window_handles[0])
            self.sgv.disconnect()

    # --------------------------------------------------
    def from_external_report_temp(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df.iloc[0,0][:36]
        if read_title == self._REPORT_TITLE:
            self.df = df
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df.iloc[0,0]
        if read_title == 'cod_motivo':
            df.columns = df.iloc[0] # Establecer la primera fila como nombres de columna
            df = df[1:]             # Eliminar la primera fila del DataFrame original
            self.df = df 
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(
        self, ejercicio: str = None, cod_motivo: str = None, motivo: str = None
    ) -> pd.DataFrame:
        """"Transform read xls file"""
        df = self.df
        df['ejercicio'] = ejercicio
        df['cod_motivo'] = cod_motivo.zfill(3)
        df['motivo'] = motivo
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
        
# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write GV's Informe Motivo Entrega Viviendas report",
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
        sgv = SaldoMotivoPorBarrio(sgv = sgv_connection)
        sgv.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        sgv_connection.disconnect()
        sgv_connection.remove_html_files(dir_path)
    else:
        sgv = SaldoMotivoPorBarrio()

    if not isinstance(args.ejercicio, list):
        file = [args.ejercicio]
    else:
        file = args.ejercicio

    file = file[0] + '-RecuperosInformeEvoSaldosPorMotivoPorBarrio.xlsx'

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
    # python -m invicodatpy.sgv.saldo_motivo_por_barrio -e 2023 --no-download