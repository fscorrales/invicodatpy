#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write Gestion Viviendas's 
        Informe de Saldos por Barrio report
"""

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field
import numpy as np

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC

from ..models.sgo_model import SGOModel
from ..utils.rpw_utils import RPWUtils
from .connect_sgo import ConnectSGO

@dataclass
class ListadoObras(RPWUtils):
    """Read, process and write Gestion Obras's Listado de Obras report"""
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='Codigo Obra'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='listado_obras'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default='cod_obra'
    )
    _SQL_MODEL:SGOModel = field(
        init=False, repr=False, default=SGOModel
    )
    sgo:ConnectSGO = field(
        init=True, repr=False, default=None
    )

    # --------------------------------------------------
    def download_report(
        self, dir_path:str
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
            self.sgo.driver.execute_cdp_cmd('Page.setDownloadBehavior', params)

            # Click botón Obras
            # btn_obras = self.sgo.driver.find_element(
            #             By.XPATH, "//*[@id='header']/ul[1]/li[1]/div/a"
            #         )
            # btn_obras.click()
            # time.sleep(1)
            # self.sgo.wait.until(EC.presence_of_element_located((
            #     By.XPATH, "//*[@id='grillaObras']/tbody/tr[2]"
            # )))

            # Llenado de inputs
            btn_exportar_completo = self.sgo.driver.find_element(
                        By.XPATH, "//*[@id='undefined-sticky-wrapper']/div/div[5]/a[3]"
                    )
            btn_exportar_completo.click()
            time.sleep(1)
            self.sgo.wait.until(EC.number_of_windows_to_be(1))
            time.sleep(5)
            self.sgo.rename_report(
                dir_path, 
                'ObrasCompleto*.xls', 
                'Obras Completo.xls'
            )
            # self.sgo.driver.close()

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            # self.sgo.driver.switch_to.window(self.sgo.driver.window_handles[0])
            self.sgo.disconnect()

    # --------------------------------------------------
    def from_external_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SGV's report"""
        df = self.read_xls(xls_path)
        read_title = df.iloc[3,0]
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
        df = df.iloc[4:, :]

        df = df.rename({
            '0': 'cod_obra', 
            '1': 'mes_basico_obra',
            '2': 'cod_contrato',
            '3': 'obra',
            '4': 'mes_basico_contrato',
            '5': 'tipo_obra',
            '6': 'localidad',
            '7': 'contratista',
            '8': 'activa',
            '9': 'monto',
            '10': 'monto_total',
            '11': 'representante',
            '12': 'operatoria',
            '13': 'q_rubros',
            '14': 'id_inspector',
            '15': 'iniciador',
            '16': 'estado',
            '17': 'fecha_inicio',
            '18': 'fecha_contrato',
            '19': 'borrar',
            '20': 'fecha_fin',
            '21': 'plazo_est_dias',
            '22': 'fecha_fin_est',
            '23': 'plazo_ampl_est_dias',
            '24': 'fecha_fin_ampl_est',
            '25': 'avance_fis_real',
            '26': 'avance_fciero_real',
            '27': 'avance_fis_est',
            '28': 'avance_fciero_est',
            '29': 'monto_certificado',
            '30': 'monto_certificado_obra',
            '31': 'monto_pagado',
            '32': 'borrar2',
            '33': 'nro_ultimo_certif',
            '34': 'nro_ultimo_certif_bc',
            '35': 'mes_obra_certif',
            '36': 'año_obra_certif',
            '37': 'fecha_ultimo_certif',
            '38': 'anticipo_acum',
            '39': 'cant_anticipo',
            '40': 'porc_anticipo',
            '41': 'cant_certif_anticipo',
            '42': 'fdo_reparo_acum',
            '43': 'desc_fdo_reparo_acum',
            '44': 'monto_redeterminado',
            '45': 'nro_ultima_rederterminacion',
            '46': 'mes_ultimo_basico',
            '47': 'año_ultimo_basico',
            '48': 'nro_ultima_medicion',
            '49': 'mes_ultima_medicion',
            '50': 'año_ultima_medicion',
            }, axis='columns')

        df[['mes_basico_obra','año_basico_obra']] = df['mes_basico_obra'].str.split(pat='/', n=1, expand=True)
        df['mes_basico_obra'] = df['mes_basico_obra'].astype(str)
        df['año_basico_obra'] = df['año_basico_obra'].astype(str)
        df['mes_basico_obra'] = df['mes_basico_obra'].str.zfill(2) + '/' + df['año_basico_obra']
        df[['mes_basico_contrato','año_basico_contrato']] = df['mes_basico_contrato'].str.split(pat='/', n=1, expand=True)
        df['mes_basico_contrato'] = df['mes_basico_contrato'].astype(str)
        df['año_basico_contrato'] = df['año_basico_contrato'].astype(str)
        df['mes_basico_contrato'] = df['mes_basico_contrato'].str.zfill(2) + '/' + df['año_basico_contrato']

        df['mes_obra_certif'] = np.where(
            df['mes_obra_certif'] != '', 
            df['mes_obra_certif'].str.zfill(2) + '/' + df['año_obra_certif'], 
            ''
        )
        df['mes_ultimo_basico'] = np.where(
            df['mes_ultimo_basico'] != '', 
            df['mes_ultimo_basico'].str.zfill(2) + '/' + df['año_ultimo_basico'], 
            ''
        )
        df['mes_ultima_medicion'] = np.where(
            df['mes_ultima_medicion'] != '', 
            df['mes_ultima_medicion'].str.zfill(2) + '/' + df['año_ultima_medicion'], 
            ''
        )

        df = df.drop(
            ['activa','borrar', 'borrar2', 'año_obra_certif', 
            'año_ultimo_basico', 'año_ultima_medicion',
            'año_basico_obra', 'año_basico_contrato'], 
            axis=1
        )

        # Formateamos columnas
        to_numeric_cols = [
            'monto', 'monto_total', 
            'avance_fis_real', 'avance_fciero_real',
            'avance_fis_est', 'avance_fciero_est', 
            'monto_certificado', 'monto_certificado_obra', 'monto_pagado',
            'anticipo_acum', 'porc_anticipo',
            'fdo_reparo_acum', 'desc_fdo_reparo_acum', 'monto_redeterminado',
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(pd.to_numeric)
        to_numeric_cols = [
            'avance_fis_real', 'avance_fciero_real',
            'avance_fis_est', 'avance_fciero_est', 
            'anticipo_acum', 'porc_anticipo',
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(lambda x: x.round(4))
        
        # to_integer_cols= [
        #     'id_inspector'
        #     # 'q_rubros', 'id_inspector', 
        #     # # 'plazo_est_dias', 'plazo_ampl_est_dias',
        #     # # 'nro_ultimo_certif', 'nro_ultimo_certif_bc', 
        #     # # 'cant_anticipo', 'cant_certif_anticipo',
        #     # # 'nro_ultima_rederterminacion', 'nro_ultima_medicion',
        # ]
        # df[to_integer_cols] = df[to_integer_cols].astype(int)
        # for col in to_integer_cols:
        #     df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        to_date_cols= [
            'fecha_inicio', 'fecha_fin', 'fecha_contrato', 'fecha_fin_est',
            'fecha_fin_ampl_est', 'fecha_ultimo_certif'

        ]
        df[to_date_cols] = df[to_date_cols].apply(pd.to_datetime)

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write GV's Informe de Saldos por Barrio report",
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
            sgo_connection = ConnectSGO(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    sgo_connection = ConnectSGO(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        sgo = ListadoObras(sgo = sgo_connection)
        sgo.download_report(
            dir_path
        )
        sgo_connection.disconnect()
        sgo_connection.remove_html_files(dir_path)
    else:
        sgo = ListadoObras()

    file = 'Obras Completo.xls'

    sgo.from_external_report(dir_path + '/' + file)
    # sgo.test_sql(dir_path + '/test.sqlite')
    sgo.to_sql(dir_path + '/sgo.sqlite', replace=True)
    sgo.print_tidyverse()
    sgo.from_sql(dir_path + '/sgo.sqlite')
    sgo.print_tidyverse()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgo.listado_obras
