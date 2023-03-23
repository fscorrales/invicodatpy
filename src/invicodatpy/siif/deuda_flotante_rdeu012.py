#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rdeu012 report
"""

import argparse
import datetime as dt
from datetime import timedelta
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
class DeudaFlotanteRdeu012(RPWUtils):
    """
    Read, process and write SIIF's rdeu012 report
    :param siif_connection must be initialized first in order to download from SIIF
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='DETALLE DE COMPROBANTES DE GASTOS ORDENADOS Y NO PAGADOS (DEUDA FLOTANTE)'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='deuda_flotante_rdeu012'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default='mes_hasta'
    )
    _SQL_MODEL:SIIFModel = field(
        init=False, repr=False, default=SIIFModel
    )
    siif:ConnectSIIF = field(
        init=True, repr=False, default=None
    )

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, 
        meses:list = dt.datetime.strftime(dt.datetime.now(), '%Y-%m')
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

            # Select rdeu012 report
            input_filter = self.siif.driver.find_element(
                By.XPATH, "//input[@id='_afrFilterpt1_afr_pc1_afr_tableReportes_afr_c1::content']"
            )
            input_filter.clear()
            input_filter.send_keys('267', Keys.ENTER)
            btn_siguiente = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:pc1:btnSiguiente']")
            btn_siguiente.click()

            # Llenado de inputs
            self.siif.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[@id='pt1:inputText3::content']")
            ))
            input_cod_fuente = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:inputText3::content']"
                )
            input_fecha_desde = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:idFechaDesde::content']"
                )
            input_fecha_hasta = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:idFechaHasta::content']"
                )
            input_cod_fuente.send_keys('0')
            input_fecha_desde.send_keys('01/01/2010')
            btn_get_reporte = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:btnVerReporte']")
            btn_xls = self.siif.driver.find_element(By.XPATH, "//input[@id='pt1:rbtnXLS::content']")
            btn_xls.click()
            if not isinstance(meses, list):
                meses = [meses]
            for mes in meses:
                int_ejercicio = int(mes[0:4])
                if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                    # Fecha Hasta
                    input_fecha_hasta.clear()
                    fecha_hasta = dt.datetime(year=(int_ejercicio), month=int(mes[-2:]), day=1)
                    next_month = fecha_hasta.replace(day=28) + timedelta(days=4)
                    fecha_hasta = next_month - timedelta(days=next_month.day)
                    fecha_hasta = min(fecha_hasta.date(), dt.date.today())
                    fecha_hasta = dt.datetime.strftime(fecha_hasta, '%d/%m/%Y')
                    input_fecha_hasta.send_keys(fecha_hasta)
                    btn_get_reporte.click()
                    self.siif.rename_report(dir_path, 'rdeu012.xls', mes[0:4] + mes[-2:] + '-rdeu012.xls')
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
        read_title = df['2'].iloc[9]
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
        df['6'] = df['6'].replace(to_replace='TODOS', value='') 
        df.loc[df['6'] != '27', 'fuente'] = df['6']
        df['fecha_desde'] = df.iloc[15,2].split(' ')[2]
        df['fecha_hasta'] = df.iloc[15,2].split(' ')[6]
        df['fecha_desde'] = pd.to_datetime(
            df['fecha_desde'], format='%d/%m/%Y'
        )
        df['fecha_hasta'] = pd.to_datetime(
            df['fecha_hasta'], format='%d/%m/%Y'
        )
        df['mes_hasta'] = (df['fecha_hasta'].dt.month.astype(str).str.zfill(2) +
                            '/'+ df['fecha_hasta'].dt.year.astype(str))
        df = df.replace(to_replace='', value=None)      
        df = df >> \
            base.tail(-13) >> \
            tidyr.fill(f.fuente) >> \
            tidyr.drop_na(f['2']) >> \
            tidyr.drop_na(f['18']) >> \
            dplyr.transmute(
                fuente = f.fuente,
                fecha_desde = f.fecha_desde,
                fecha_hasta = f.fecha_hasta,
                mes_hasta = f.mes_hasta,
                nro_entrada = f['2'],
                nro_origen = f['4'],
                fecha_aprobado = f['7'],
                org_fin = f['9'],
                importe = base.as_double(f['10']),
                saldo = base.as_double(f['13']),
                nro_expte = f['14'],
                cta_cte = f['15'],
                glosa = f['17'],
                cuit = f['18'],
                beneficiario = f['19']
            )

        df['fecha_aprobado'] = pd.to_datetime(
            df['fecha_aprobado'], format='%Y-%m-%d'
        ) 

        df = df >> \
            dplyr.mutate(
                fecha = dplyr.if_else(f.fecha_aprobado > f.fecha_hasta,
                                        f.fecha_hasta, f.fecha_aprobado)
            )

        # CYO aprobados en enero correspodientes al ejercicio anterior
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce') 
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
        condition = ((df['mes_hasta'].str[0:2] == '01') & 
                    (df['nro_entrada'].astype(int) > 1500))
        df.loc[condition, 'fecha'] = (
            (pd.to_numeric(df['mes_hasta'].loc[condition].str[-4:]) - 1).astype(str) + "-12-31")

        df['fecha'] = pd.to_datetime(
            df['fecha'],  format='%Y-%m-%d', errors='coerce'
        ) 
        
        df['ejercicio'] = df['fecha'].dt.year.astype(str)
        df['mes'] = df['fecha'].dt.strftime('%m/%Y')

        df = df >>\
            dplyr.mutate(
                nro_comprobante = (f['nro_entrada'].str.zfill(5) + 
                                '/' + f['mes'].str[-2:]),
            ) >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha, 
                f.mes_hasta, f.fuente,
                f.cta_cte, f.nro_comprobante,
                f.importe, f.saldo,
                f.cuit, f.beneficiario,
                f.glosa, f.nro_expte,
                f.nro_entrada, f.nro_origen,
                dplyr.everything()
            )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rdeu012 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='',
        type=str,
        help = "SIIF' rdeu012.xls report. Must be in the same folder")

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
        '-e', '--mes', 
        metavar = 'Año y mes',
        default = '202212',
        type=str,
        help = "Año y mes en formato yyyymm")

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
        siif_rdeu012 = DeudaFlotanteRdeu012(siif = siif_connection)
        siif_rdeu012.download_report(
            dir_path, meses=args.mes
        )
        siif_connection.disconnect()
    else:
        siif_rdeu012 = DeudaFlotanteRdeu012()

    if args.file != '':
        filename = args.file
    else:
        filename = args.mes[0:4] + args.mes[-2:] + '-rdeu012.xls'

    siif_rdeu012.from_external_report(dir_path + '/' + filename)
    # siif_rdeu012.test_sql(dir_path + '/test.sqlite')
    siif_rdeu012.to_sql(dir_path + '/siif.sqlite')
    siif_rdeu012.print_tidyverse()
    siif_rdeu012.from_sql(dir_path + '/siif.sqlite')
    siif_rdeu012.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.deuda_flotante_rdeu012