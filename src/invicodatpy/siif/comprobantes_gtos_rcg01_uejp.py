#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rcg01_uejp report
"""

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field

import pandas as pd
from datar import base, dplyr, f, tidyr
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from ..models.siif_model import SIIFModel
from ..utils.rpw_utils import RPWUtils
from .connect_siif import ConnectSIIF

@dataclass
class ComprobantesGtosRcg01Uejp(RPWUtils):
    """
    Read, process and write SIIF's rcg01_uejp report
    :param siif_connection must be initialized first in order to download from SIIF
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='Resumen Diario de Comprobantes de Gastos Ingresados'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='comprobantes_gtos_rcg01_uejp'
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
            input_filter.send_keys('839', Keys.ENTER)
            btn_siguiente = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:pc1:btnSiguiente']")
            btn_siguiente.click()

            # Llenado de inputs
            self.siif.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[@id='pt1:txtAnioEjercicio::content']")
            ))
            input_ejercicio = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtAnioEjercicio::content']"
                )
            input_fecha_desde = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:idFechaDesde::content']"
                )
            input_fecha_hasta = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:idFechaHasta::content']"
                )
            input_unidad_ejecutora = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtUnidadEjecutora::content']"
                )
            input_unidad_ejecutora.send_keys('0')
            btn_get_reporte = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:btnVerReporte']")
            btn_xls = self.siif.driver.find_element(By.XPATH, "//input[@id='pt1:rbtnXLS::content']")
            btn_xls.click()

            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                    # Ejercicio
                    input_ejercicio.clear()
                    input_ejercicio.send_keys(ejercicio)
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
                    self.siif.rename_report(dir_path, 'rcg01_uejp.xls', ejercicio + '-rcg01_uejp.xls')
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

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['1'].iloc[4]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read xls file"""
        self.df = self.df.replace(to_replace='', value=None)      
        df = self.df >> \
            dplyr.mutate(
                ejercicio = f['1'].iloc[2][-4:]
            ) >> \
            dplyr.select(~f['0'], ~f['17'], ~f['18']) >> \
            base.tail(-16)

        df.columns = [
            "nro_entrada", "nro_origen", "fuente", "clase_reg",
            "clase_mod", "clase_gto", "fecha", "importe",
            "cuit", "beneficiario", "nro_expte","cta_cte",
            "es_comprometido", "es_verificado", "es_aprobado", "es_pagado",
            "nro_fondo", "ejercicio"
        ]

        df = df >> \
            tidyr.drop_na(f.cuit) >> \
            tidyr.drop_na(f.nro_entrada) >> \
            dplyr.mutate(
                importe = base.as_double(f.importe),
                beneficiario = f.beneficiario.str.replace("\t", ""),
                es_comprometido = dplyr.if_else(f.es_comprometido == "S", True, False),
                es_verificado = dplyr.if_else(f.es_verificado == "S", True, False),
                es_aprobado = dplyr.if_else(f.es_aprobado == "S", True, False),
                es_pagado = dplyr.if_else(f.es_pagado == "S", True, False)
            )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%Y-%m-%d'
        )
        df['mes'] = df['fecha'].dt.strftime('%m/%Y')
        df['nro_comprobante'] = df['nro_entrada'].str.zfill(5) + '/' + df['mes'].str[-2:]

        df = df >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha,
                f.nro_comprobante, f.importe,
                f.fuente, f.cta_cte, f.cuit,
                f.nro_expte, f.nro_fondo,
                dplyr.everything()
            )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rcg01_uejp report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='2022-rcg01_uejp.xls',
        type=str,
        help = "SIIF' rcg01_uejp.xls report. Must be in the same folder")
    parser.add_argument(
        '-d', '--download', 
        metavar = "download",
        default=True,
        type=bool,
        help = "Download data")

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
        siif_rcg01_uejp = ComprobantesGtosRcg01Uejp(siif = siif_connection)
        siif_rcg01_uejp.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        siif_connection.disconnect()
    else:
        siif_rcg01_uejp = ComprobantesGtosRcg01Uejp()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + '-rf602.xls'
        
    siif_rcg01_uejp.from_external_report(dir_path + '/' + args.file)
    # siif_rcg01_uejp.test_sql(dir_path + '/test.sqlite')
    siif_rcg01_uejp.to_sql(dir_path + '/siif.sqlite')
    siif_rcg01_uejp.print_tidyverse()
    siif_rcg01_uejp.from_sql(dir_path + '/siif.sqlite')
    siif_rcg01_uejp.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.comprobantes_gtos_rcg01_uejp
