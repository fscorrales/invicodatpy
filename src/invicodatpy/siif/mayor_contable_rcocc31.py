#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rcocc31 report
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
from selenium.webdriver.support.ui import Select

from ..models.siif_model import SIIFModel
from ..utils.rpw_utils import RPWUtils
from .connect_siif import ConnectSIIF

@dataclass
class MayorContableRcocc31(RPWUtils):
    """
    Read, process and write SIIF's rcocc31 report
    :param siif_connection must be initialized first in order to download from SIIF
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='DETALLES DE MOVIMIENTOS CONTABLES'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='mayor_contable_rcocc31'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default_factory=lambda: ['mes', 'cta_contable']
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
        self, dir_path:str, 
        ejercicios:list = str(dt.datetime.now().year),
        ctas_contables:list = '1112-2-6'
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
            cmb_modulos.select_by_visible_text('SUB - SISTEMA DE CONTABILIDAD PATRIMONIAL')
            time.sleep(1)

            # Select rcocc31 report
            input_filter = self.siif.driver.find_element(
                By.XPATH, "//input[@id='_afrFilterpt1_afr_pc1_afr_tableReportes_afr_c1::content']"
            )
            input_filter.clear()
            input_filter.send_keys('387', Keys.ENTER)
            btn_siguiente = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:pc1:btnSiguiente']")
            btn_siguiente.click()

            # Llenado de inputs
            self.siif.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[@id='pt1:txtAnioEjercicio::content']")
            ))
            input_ejercicio = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtAnioEjercicio::content']"
                )
            input_nivel = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtNivel::content']"
                )
            input_mayor = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtMayor::content']"
                )
            input_subcuenta = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtSubCuenta::content']"
                )
            input_fecha_desde = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:idFechaDesde::content']"
                )
            input_fecha_hasta = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:idFechaHasta::content']"
                )
            btn_get_reporte = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:btnEjecutarReporte']")
            btn_xls = self.siif.driver.find_element(By.XPATH, "//input[@id='pt1:rbtnXLS::content']")
            btn_xls.click()
            if not isinstance(ctas_contables, list):
                ctas_contables = [ctas_contables]
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                    for cta_contable in ctas_contables:
                        # Ejercicio
                        input_ejercicio.clear()
                        input_ejercicio.send_keys(ejercicio)
                        # Nivel
                        input_nivel.clear()
                        input_nivel.send_keys(cta_contable[0:4])
                        # Nivel
                        input_mayor.clear()
                        input_mayor.send_keys(cta_contable[5])
                        # Nivel
                        input_subcuenta.clear()
                        input_subcuenta.send_keys(cta_contable[7])
                        # Fecha Desde
                        input_fecha_desde.clear()
                        fecha_desde = dt.datetime.strftime(
                            dt.date(year=int_ejercicio, month=1, day=1),
                            '%d/%m/%Y'
                        )
                        input_fecha_desde.send_keys(fecha_desde)
                        # Fecha Hasta
                        input_fecha_hasta.clear()
                        fecha_hasta = dt.datetime(year=(int_ejercicio+1), month=12, day=31)
                        fecha_hasta = min(fecha_hasta, dt.datetime.now())
                        fecha_hasta = dt.datetime.strftime(fecha_hasta, '%d/%m/%Y'
                        )
                        input_fecha_hasta.send_keys(fecha_hasta)
                        btn_get_reporte.click()
                        self.siif.rename_report(
                            dir_path, 'rcocc31.xls', 
                            ejercicio + '-rcocc31 ('+ cta_contable +').xls'
                        )
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
        read_title = df['2'].iloc[9][:33]
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
        df['ejercicio'] = df.iloc[3,1][-4:]
        df['cta_contable'] = (df.iloc[10,6] + '-' + 
        df.iloc[10,11] + '-' + df.iloc[10,12])
        df = df.replace(to_replace='', value=None)
        df = df.iloc[20:, :]
        df = df.rename({
            '3': 'nro_entrada', 
            '10': 'nro_original',
            '14': 'fecha_aprobado',
            '19': 'auxiliar_1',
            '22': 'auxiliar_2',
            '25': 'tipo_comprobante',
            '26': 'debitos',
            '28': 'creditos',
            '29': 'saldo',
            }, axis='columns')
        df = df.dropna(subset=['nro_entrada'])
        df['fecha_aprobado'] = pd.to_datetime(
            df['fecha_aprobado'], format='%Y-%m-%d'
        )
        df['fecha'] = df['fecha_aprobado']
        # df.loc[df['fecha_aprobado'].dt.year.astype(str) == df['ejercicio'], 'fecha'] = df['fecha_aprobado']
        df.loc[df['fecha_aprobado'].dt.year.astype(str) != df['ejercicio'], 'fecha'] = pd.to_datetime(
            df['ejercicio'] + '-12-31', format='%Y-%m-%d'
        )
        df['mes'] = df['fecha'].dt.month.astype(str).str.zfill(2) + '/' + df['ejercicio']

        df = df.loc[
            :, ['ejercicio', 'mes', 'fecha', 'fecha_aprobado', 
                'cta_contable', 'nro_entrada', 'nro_original', 
                'auxiliar_1', 'auxiliar_2',
                'tipo_comprobante', 'debitos', 'creditos', 'saldo']
        ]
        to_numeric_cols = [
            'debitos', 'creditos', 'saldo'
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric)      
        # df = df >> \
        #     base.tail(-20) >> \
        #     tidyr.drop_na(f['3']) >> \
        #     dplyr.transmute(
        #         ejercicio = f.ejercicio,
        #         cta_contable = f.cta_contable, 
        #         nro_entrada = f['3'], 
        #         fecha_aprobado = f['9'],
        #         auxiliar_1 = f['14'],
        #         auxiliar_2 = f['19'],
        #         tipo_comprobante = f['21'],
        #         debitos = base.as_double(f['24']), 
        #         creditos = base.as_double(f['25']),
        #         saldo = base.as_double(f['27'])
        #     )

        # df['fecha_aprobado'] = pd.to_datetime(
        #     df['fecha_aprobado'], format='%Y-%m-%d'
        # )
        # # df['fecha'] = df['fecha_aprobado'].dt.year.astype(str).apply(
        # #     lambda x: df['fecha_aprobado'] 
        # #     if x == df['ejercicio'] 
        # #     else pd.to_datetime(df['ejercicio'] + '-12-31', format='%Y-%m-%d')
        # #     )
        # df.loc[df['fecha_aprobado'].dt.year.astype(str) == df['ejercicio'], 'fecha'] = df['fecha_aprobado']
        # df.loc[df['fecha_aprobado'].dt.year.astype(str) != df['ejercicio'], 'fecha'] = pd.to_datetime(
        #     df['ejercicio'] + '-12-31', format='%Y-%m-%d'
        # )

        # df = df >>\
        #     dplyr.mutate(
        #         mes = df['fecha'].dt.month.astype(str).str.zfill(2) + '/' + df['ejercicio']
        #     ) >> \
        #     dplyr.select(
        #         f.ejercicio, f.mes, f.fecha, f.fecha_aprobado,
        #         dplyr.everything()
        #     )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rcocc31 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='',
        type=str,
        help = "SIIF' rcocc31.xls report. Must be in the same folder")

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
            siif_connection = ConnectSIIF(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    siif_connection = ConnectSIIF(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        siif_rcocc31 = MayorContableRcocc31(siif = siif_connection)
        siif_rcocc31.connect()
        siif_rcocc31.go_to_reports()
        siif_rcocc31.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        siif_connection.disconnect()
        siif_connection.remove_html_files(dir_path)
    else:
        siif_rcocc31 = MayorContableRcocc31()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + '-rcocc31 (1112-2-6).xls'

    siif_rcocc31.from_external_report(dir_path + '/' + filename)
    # siif_rcocc31.test_sql(dir_path + '/test.sqlite')
    siif_rcocc31.to_sql(dir_path + '/siif.sqlite')
    siif_rcocc31.print_tidyverse()
    siif_rcocc31.from_sql(dir_path + '/siif.sqlite')
    siif_rcocc31.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.mayor_contable_rcocc31 --no-download