#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's gto_rpa03g report
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
class ComprobantesGtosGpoPartGtoRpa03g(RPWUtils):
    """
    Read, process and write SIIF's gto_rpa03g report
    :param siif_connection must be initialized first in order to download from SIIF
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='DETALLE DE DOCUMENTOS ORDENADOS. PARTIDA'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='comprobantes_gtos_gpo_part_gto_rpa03g'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default_factory=lambda: ['mes', 'grupo']
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
            input_filter.send_keys('1175', Keys.ENTER)
            btn_siguiente = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:pc1:btnSiguiente']")
            btn_siguiente.click()

            # Llenado de inputs
            self.siif.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[@id='pt1:txtAnioEjercicio::content']")
            ))
            input_ejercicio = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtAnioEjercicio::content']"
                )
            input_gpo_partida = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtGrupoPartida::content']"
                )
            input_mes_desde = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtMesDesde::content']"
                )
            input_mes_hasta = self.siif.driver.find_element(
                    By.XPATH, "//input[@id='pt1:txtMesHasta::content']"
                )
            input_mes_desde.send_keys('1')
            input_mes_hasta.send_keys('12')
            btn_get_reporte = self.siif.driver.find_element(By.XPATH, "//div[@id='pt1:btnVerReporte']")
            btn_xls = self.siif.driver.find_element(By.XPATH, "//input[@id='pt1:rbtnXLS::content']")
            btn_xls.click()
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                    for grupo_partida in ['1', '2', '3', '4']:
                        # Ejercicio
                        input_ejercicio.clear()
                        input_ejercicio.send_keys(ejercicio)
                        # Grupo Partida
                        input_gpo_partida.clear()
                        input_gpo_partida.send_keys(grupo_partida)
                        btn_get_reporte.click()
                        self.siif.rename_report(
                            dir_path, 'gto_rpa03g.xls', 
                            ejercicio + '-gto_rpa03g (Gpo '+ grupo_partida +'00).xls'
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

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['18'].iloc[5][:40]
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
        df['ejercicio'] = df.iloc[3,18][-4:]
        df = df.replace(to_replace='', value=None)      
        df = df >> \
            base.tail(-21) >> \
            tidyr.drop_na(f['1']) >> \
            dplyr.transmute(
                ejercicio = f.ejercicio,
                nro_entrada = f['1'],
                nro_origen = f['5'],
                importe = base.as_double(f['8']),
                fecha = f['14'],
                partida =  f['17'],
                grupo = f.partida.str[0] + '00',
                nro_expte = f['19'],
                glosa = f['21'],
                beneficiario = f['23'],
                mes =  f.fecha.str[5:7] + '/' + f.ejercicio,
                nro_comprobante = f['nro_entrada'].str.zfill(5) + '/' + f['mes'].str[-2:]
            ) >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha,
                f.nro_comprobante, f.importe,
                f.grupo, f.partida,
                dplyr.everything()
            )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%Y-%m-%d'
        )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's gto_rpa03g report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='',
        type=str,
        help = "SIIF' gto_rpa03g.xls report. Must be in the same folder")

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
        siif_gto_rpa03g = ComprobantesGtosGpoPartGtoRpa03g(siif = siif_connection)
        siif_gto_rpa03g.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        siif_connection.disconnect()
    else:
        siif_gto_rpa03g = ComprobantesGtosGpoPartGtoRpa03g()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + '-gto_rpa03g (Gpo 400).xls'
    
    siif_gto_rpa03g.from_external_report(dir_path + '/' + filename)
    # siif_gto_rpa03g.test_sql(dir_path + '/test.sqlite')
    siif_gto_rpa03g.to_sql(dir_path + '/siif.sqlite')
    siif_gto_rpa03g.print_tidyverse()
    siif_gto_rpa03g.from_sql(dir_path + '/siif.sqlite')
    siif_gto_rpa03g.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.comprobantes_gtos_gpo_part_gto_rpa03g
