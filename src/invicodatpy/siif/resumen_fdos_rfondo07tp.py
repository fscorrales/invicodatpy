#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rfondo07tp report
"""

__all__ = ['ResumenFdosRfondo07tp']

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
class ResumenFdosRfondo07tp(ConnectSIIF):
    """
    Read, process and write SIIF's rfondo07tp report
    :param siif_connection must be initialized first in order to download from SIIF
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='RESUMEN DE FONDOS DEL EJERCICIO'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='resumen_fdos_rfondo07tp'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default_factory=lambda:['mes', 'tipo_comprobante']
    )
    _SQL_MODEL:SIIFModel = field(
        init=False, repr=False, default=SIIFModel
    )


    # --------------------------------------------------
    def download_report(
        self, dir_path:str, ejercicios:list = str(dt.datetime.now().year), 
        tipo_comprobante:str = 'PA6'
    ):
        try:
            self.set_download_path(dir_path)
            self.select_report_module(ReportCategory.Gastos)
            self.select_specific_report_by_id('2070')

            # Getting DOM elements
            input_ejercicio = self.get_dom_element(
                "//input[@id='pt1:txtAnioEjercicio::content']", wait=True
                )
            input_fecha_desde = self.get_dom_element(
                "//input[@id='pt1:idFechaDesde::content']"
                )
            input_fecha_hasta = self.get_dom_element(
                "//input[@id='pt1:idFechaHasta::content']"
                )
            input_tipo_comprobante = self.get_dom_element(
                "//input[@id='pt1:txtTipoCte::content']"
                )
            btn_get_reporte = self.get_dom_element(
                "//div[@id='pt1:btnVerReporte']"
                )
            btn_xls = self.get_dom_element(
                "//input[@id='pt1:rbtnXLS::content']"
                )
            btn_xls.click()

            # Form submit
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
                    # Tipo Comprobante
                    input_tipo_comprobante.clear()
                    input_tipo_comprobante.send_keys(tipo_comprobante)
                    btn_get_reporte.click()

                    # Download and rename xls
                    self.rename_report(
                        dir_path, 'rfondo07tp.xls', 
                        ejercicio + '-rfondo07tp ('+ tipo_comprobante +').xls'
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
        read_title = df['1'].iloc[4][:-5]
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
        df['ejercicio'] = df.iloc[4,1][-4:]
        df['tipo_comprobante'] = df.iloc[11,2].split(':')[2].strip()
        # df['tipo_comprobante'] = base.trimws(
        #     df.iloc[11,2].split(':')[2]
        # )
        df = df.replace(to_replace='', value=None)
        df = df.tail(-19)
        df = df.dropna(subset=['10'])
        df = df.rename(columns={
            '3': 'nro_fondo',
            '6': 'glosa',
            '10': 'fecha',
            '12': 'ingresos',
            '15': 'egresos',
            '18': 'saldo',
        })
        df['mes'] = df['fecha'].str[5:7] + '/' + df['ejercicio']
        df['nro_comprobante'] = df['nro_fondo'].str.zfill(5) + '/' + df['mes'].str[-2:]
        # df = df >> \
        #     base.tail(-19) >> \
        #     tidyr.drop_na(f['10']) >> \
        #     dplyr.transmute(
        #         ejercicio = f.ejercicio,
        #         tipo_comprobante = base.trimws(f.tipo_comprobante),
        #         fecha = f['10'],
        #         mes =  f.fecha.str[5:7] + '/' + f.ejercicio,
        #         nro_fondo = f['3'],
        #         glosa = base.trimws(f['6']),
        #         ingresos = base.as_double(f['12']),
        #         egresos = base.as_double(f['15']),
        #         saldo = base.as_double(f['18']),
        #         nro_comprobante = f['nro_fondo'].str.zfill(5) + '/' + f['mes'].str[-2:]
        #     )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%Y-%m-%d'
        )

        df = df.loc[:, [
            'ejercicio', 'mes', 'fecha', 'tipo_comprobante', 'nro_comprobante',
            'nro_fondo', 'glosa', 'ingresos', 'egresos', 'saldo'
        ]]

        to_numeric_cols = [
            'ingresos', 'egresos', 'saldo'
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric).astype(np.float64) 

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rfondo07tp report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='',
        type=str,
        help = "SIIF' rfondo07tp.xls report. Must be in the same folder")

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
        siif = ResumenFdosRfondo07tp()
        siif.go_to_reports()
        siif.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        siif.disconnect()
    else:
        siif = ResumenFdosRfondo07tp()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + '-rfondo07tp (PA6).xls'

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
    # python -m invicodatpy.siif.resumen_fdos_rfondo07tp
    # python -m invicodatpy.siif.resumen_fdos_rfondo07tp --no-download
