#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's gto_rpa03g report
"""

__all__ = ['ComprobantesGtosGpoPartGtoRpa03g']

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
class ComprobantesGtosGpoPartGtoRpa03g(ConnectSIIF):
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

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, 
        ejercicios:list = str(dt.datetime.now().year),
        group_part:list = ['1','2','3','4']
    ):
        try:
            self.set_download_path(dir_path)
            self.select_report_module(ReportCategory.Gastos)
            self.select_specific_report_by_id('1175')
            
            # Getting DOM elements
            input_ejercicio = self.get_dom_element(
                "//input[@id='pt1:txtAnioEjercicio::content']", wait=True
            )
            input_gpo_partida = self.get_dom_element(
                "//input[@id='pt1:txtGrupoPartida::content']"
            )
            input_mes_desde = self.get_dom_element(
                "//input[@id='pt1:txtMesDesde::content']"
            )
            input_mes_hasta = self.get_dom_element(
                "//input[@id='pt1:txtMesHasta::content']"
            )
            btn_get_reporte = self.get_dom_element(
                "//div[@id='pt1:btnVerReporte']"
            )
            btn_xls = self.get_dom_element(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            btn_xls.click()

            # Form submit
            input_mes_desde.send_keys('1')
            input_mes_hasta.send_keys('12')
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                    for grupo_partida in group_part:
                        # Ejercicio
                        input_ejercicio.clear()
                        input_ejercicio.send_keys(ejercicio)
                        # Grupo Partida
                        input_gpo_partida.clear()
                        input_gpo_partida.send_keys(grupo_partida)
                        btn_get_reporte.click()

                        # Download and rename xls
                        self.rename_report(
                            dir_path, 'gto_rpa03g.xls', 
                            ejercicio + '-gto_rpa03g (Gpo '+ grupo_partida +'00).xls'
                        )
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
        read_title = df['18'].iloc[5][:40]
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
        df['ejercicio'] = df.iloc[3,18][-4:]
        df = df.replace(to_replace='', value=None)
        df = df.tail(-21)
        df = df.dropna(subset=['1'])
        df = df.rename(columns={
            '1': 'nro_entrada',
            '5': 'nro_origen',
            '8': 'importe',
            '14': 'fecha',
            '17': 'partida',
            '19': 'nro_expte',
            '21': 'glosa',
            '23': 'beneficiario',
        })
        df['importe'] = pd.to_numeric(df['importe']).astype(np.float64)
        df['grupo'] = df['partida'].str[0] + '00'
        df['mes'] = df['fecha'].str[5:7] + '/' + df['ejercicio']
        df['nro_comprobante'] = df['nro_entrada'].str.zfill(5) + '/' + df['mes'].str[-2:]

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%Y-%m-%d'
        )

        df = df.loc[:,[
            'ejercicio', 'mes', 'fecha', 'nro_comprobante', 'importe',
            'grupo', 'partida', 'nro_entrada', 'nro_origen', 'nro_expte', 
            'glosa', 'beneficiario', 
        ]]

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
            ConnectSIIF.connect(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    ConnectSIIF.connect(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        siif = ComprobantesGtosGpoPartGtoRpa03g()
        siif.go_to_reports()
        siif.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        siif.disconnect()
    else:
        siif = ComprobantesGtosGpoPartGtoRpa03g()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + '-gto_rpa03g (Gpo 400).xls'
    
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
    # python -m invicodatpy.siif.comprobantes_gtos_gpo_part_gto_rpa03g
    # python -m invicodatpy.siif.comprobantes_gtos_gpo_part_gto_rpa03g --no-download
