#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SIIF's rdeu012 report
"""

__all__ = ['DeudaFlotanteRdeu012']

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field
from datetime import timedelta

import numpy as np
import pandas as pd

from ..models.siif_model import SIIFModel
from .connect_siif import ConnectSIIF, ReportCategory


@dataclass
class DeudaFlotanteRdeu012(ConnectSIIF):
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

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, 
        meses:list = dt.datetime.strftime(dt.datetime.now(), '%Y-%m')
    ):
        try:
            self.set_download_path(dir_path)
            self.select_report_module(ReportCategory.Gastos)
            self.select_specific_report_by_id('267')
            
            # Getting DOM elements
            input_cod_fuente = self.get_dom_element(
                    "//input[@id='pt1:inputText3::content']", wait=True
                )
            input_fecha_desde = self.get_dom_element(
                    "//input[@id='pt1:idFechaDesde::content']"
                )
            input_fecha_hasta = self.get_dom_element(
                    "//input[@id='pt1:idFechaHasta::content']"
                )
            input_cod_fuente.send_keys('0')
            input_fecha_desde.send_keys('01/01/2010')
            btn_get_reporte = self.get_dom_element(
                    "//div[@id='pt1:btnVerReporte']"
                )
            btn_xls = self.get_dom_element(
                    "//input[@id='pt1:rbtnXLS::content']"
                )
            btn_xls.click()

            # Form submit
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
                    self.rename_report(dir_path, 'rdeu012.xls', mes[0:4] + mes[-2:] + '-rdeu012.xls')
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
        read_title = df['2'].iloc[9]
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
        df = df.tail(-13)
        df['fuente'] = df['fuente'].fillna(method='ffill')
        df = df.dropna(subset=['2'])
        df = df.dropna(subset=['18'])  
        df = df.rename(columns={
            '2':'nro_entrada',
            '4':'nro_origen',
            '7':'fecha_aprobado',
            '9':'org_fin',
            '10':'importe',
            '13':'saldo',
            '14':'nro_expte',
            '15':'cta_cte',
            '17':'glosa',
            '18':'cuit',
            '19':'beneficiario'
        })

        to_numeric = ['importe', 'saldo']
        df[to_numeric] = df[to_numeric].apply(pd.to_numeric).astype(np.float64)

        df['fecha_aprobado'] = pd.to_datetime(
            df['fecha_aprobado'], format='%Y-%m-%d'
        )
        df['mes_aprobado'] = df['fecha_aprobado'].dt.strftime('%m/%Y')

        df['fecha'] = np.where(
            df['fecha_aprobado'] > df['fecha_hasta'],
            df['fecha_hasta'], 
            df['fecha_aprobado']
        )
        # df = df >> \
        #     dplyr.mutate(
        #         fecha = dplyr.if_else(f.fecha_aprobado > f.fecha_hasta,
        #                                 f.fecha_hasta, f.fecha_aprobado)
        #     )

        # CYO aprobados en enero correspodientes al ejercicio anterior
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce') 
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')
        condition = ((df['mes_aprobado'].str[0:2] == '01') & 
                    (df['nro_entrada'].astype(int) > 1500))
        df.loc[condition, 'fecha'] = (
            (pd.to_numeric(df['mes_hasta'].loc[condition].str[-4:])).astype(str) + "-12-31")

        df['fecha'] = pd.to_datetime(
            df['fecha'],  format='%Y-%m-%d', errors='coerce'
        ) 
        
        df['ejercicio'] = df['fecha'].dt.year.astype(str)
        df['mes'] = df['fecha'].dt.strftime('%m/%Y')

        df['nro_comprobante'] = (
            df['nro_entrada'].str.zfill(5) + '/' +  df['mes'].str[-2:]
        )
        df = df.loc[:, [
            'ejercicio', 'mes', 'fecha', 'mes_hasta', 'fuente',
            'cta_cte', 'nro_comprobante', 'importe', 'saldo',
            'cuit', 'beneficiario', 'glosa', 'nro_expte',
            'nro_entrada', 'nro_origen', 'fecha_aprobado', 
            'fecha_desde', 'fecha_hasta', 'org_fin'
        ]]

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
            ConnectSIIF(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    ConnectSIIF(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        siif = DeudaFlotanteRdeu012()
        siif.go_to_reports()
        siif.download_report(
            dir_path, meses=args.mes
        )
        siif.disconnect()
    else:
        siif = DeudaFlotanteRdeu012()

    if args.file != '':
        filename = args.file
    else:
        filename = args.mes[0:4] + args.mes[-2:] + '-rdeu012.xls'

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
    # python -m invicodatpy.siif.deuda_flotante_rdeu012 --no-download