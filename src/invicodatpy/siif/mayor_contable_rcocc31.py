#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rcocc31 report
"""

__all__ = ['MayorContableRcocc31']

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field

import pandas as pd

from ..models.siif_model import SIIFModel
from .connect_siif import ConnectSIIF, ReportCategory


@dataclass
class MayorContableRcocc31(ConnectSIIF):
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
        init=False, repr=False, default_factory=lambda: ['ejercicio', 'cta_contable']
    )
    _SQL_MODEL:SIIFModel = field(
        init=False, repr=False, default=SIIFModel
    )

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, 
        ejercicios:list = str(dt.datetime.now().year),
        ctas_contables:list = '1112-2-6'
    ):
        try:
            self.set_download_path(dir_path)
            self.select_report_module(ReportCategory.Contabilidad)
            self.select_specific_report_by_id('387')

            # Getting DOM elements
            input_ejercicio = self.get_dom_element(
                "//input[@id='pt1:txtAnioEjercicio::content']", wait=True
            )
            input_nivel = self.get_dom_element(
                "//input[@id='pt1:txtNivel::content']"
            )
            input_mayor = self.get_dom_element(
                "//input[@id='pt1:txtMayor::content']"
            )
            input_subcuenta = self.get_dom_element(
                "//input[@id='pt1:txtSubCuenta::content']"
            )
            input_fecha_desde = self.get_dom_element(
                "//input[@id='pt1:idFechaDesde::content']"
            )
            input_fecha_hasta = self.get_dom_element(
                "//input[@id='pt1:idFechaHasta::content']"
            )
            btn_get_reporte = self.get_dom_element(
                "//div[@id='pt1:btnEjecutarReporte']"
            )
            btn_xls = self.get_dom_element(
                "//input[@id='pt1:rbtnXLS::content']"
            )
            btn_xls.click()

            # Form submit
            if not isinstance(ctas_contables, list):
                ctas_contables = [ctas_contables]
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2009 and int_ejercicio <= dt.datetime.now().year:
                    for cta_contable in ctas_contables:
                        nivel, mayor, subcuenta = cta_contable.split('-')
                        # Ejercicio
                        input_ejercicio.clear()
                        input_ejercicio.send_keys(ejercicio)
                        # Nivel
                        input_nivel.clear()
                        input_nivel.send_keys(nivel)
                        # Nivel
                        input_mayor.clear()
                        input_mayor.send_keys(mayor)
                        # Nivel
                        input_subcuenta.clear()
                        input_subcuenta.send_keys(subcuenta)
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

                        # Download and rename xls
                        self.rename_report(
                            dir_path, 'rcocc31.xls', 
                            ejercicio + '-rcocc31 ('+ cta_contable +').xls'
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

    # parser.add_argument(
    #     '-c', '--cuenta', 
    #     metavar = 'Cuentas Contables',
    #     default = ['1112-2-6'],
    #     type = lambda s: [i.strip() for i in s.split(',')],
    #     help = "Cuentas Contables to download from SIIF. "
    #            "Use comas to separate multiple accounts")

    parser.add_argument(
        '-c', '--cuenta', 
        metavar = 'Cuentas Contables',
        default = ['1112-2-6'],
        nargs='*', 
        type=str,
        help = "Cuentas Contables to download from SIIF")

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
        siif = MayorContableRcocc31()
        siif.go_to_reports()
        siif.download_report(
            dir_path, ejercicios=args.ejercicio, 
            ctas_contables=args.cuenta
        )
        siif.disconnect()
    else:
        siif = MayorContableRcocc31()

    if args.file != '':
        filename = args.file
    else:
        filename = []
        for cta_contable in args.cuenta:
            filename.append(args.ejercicio + '-rcocc31 (' + cta_contable + ').xls')
            # filename = args.ejercicio + '-rcocc31 (1112-2-6).xls'

    for f in filename:
        siif.from_external_report(dir_path + '/' + f)
        # siif_rcocc31.test_sql(dir_path + '/test.sqlite')
        siif.to_sql(dir_path + '/siif.sqlite')
        siif.print_tidyverse()
        siif.from_sql(dir_path + '/siif.sqlite')
        siif.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.mayor_contable_rcocc31 --no-download