#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SSCC's 'Consulta 
General de Movimientos' report
"""

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field

import pandas as pd
# from datar import base, dplyr, f, tidyr
from pywinauto import findwindows, keyboard, mouse

from ..models.sscc_model import SSCCModel
from ..utils.rpw_utils import RPWUtils
from .connect_sscc import ConnectSSCC


@dataclass
class BancoINVICO(RPWUtils):
    """
    Read, process and write SSCC's 'Consulta General de Movimientos' report
    :param sscc_connection must be initialized first in order to automate SSCC
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='Consulta General de Movimientos'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='banco_invico'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, default='mes'
    )
    _SQL_MODEL:SSCCModel = field(
        init=False, repr=False, default=SSCCModel
    )
    sscc:ConnectSSCC = field(
        init=True, repr=False, default=None
    )

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, ejercicios:list = str(dt.datetime.now().year)
    ):
        try:
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for ejercicio in ejercicios:
                # Open menu Consulta General de Movimientos
                self.sscc.main.menu_select("Informes->Consulta General de Movimientos")

                dlg_consulta_gral_mov = self.sscc.main.child_window(
                    title="Consulta General de Movimientos (Vista No Actualizada)", control_type="Window"
                ).wait('exists')

                int_ejercicio = int(ejercicio)
                if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                    # Fecha Desde
                    ## Click on año desde
                    time.sleep(1)
                    mouse.click(coords=(495, 205))
                    keyboard.send_keys(ejercicio)
                    ## Click on mes desde
                    time.sleep(1)
                    mouse.click(coords=(470, 205))
                    keyboard.send_keys('01')
                    ## Click on día desde
                    time.sleep(1)
                    mouse.click(coords=(455, 205))
                    keyboard.send_keys('01')

                    # Fecha Hasta
                    fecha_hasta = dt.datetime(year=(int_ejercicio), month=12, day=31)
                    fecha_hasta = min(fecha_hasta, dt.datetime.now())
                    fecha_hasta = dt.datetime.strftime(fecha_hasta, '%d/%m/%Y')
                    ## Click on año hasta
                    time.sleep(1)
                    mouse.click(coords=(610, 205))
                    keyboard.send_keys(ejercicio)
                    ## Click on mes hasta
                    time.sleep(1)
                    mouse.click(coords=(590, 205))
                    keyboard.send_keys(fecha_hasta[3:5])
                    ## Click on día hasta
                    time.sleep(1)
                    mouse.click(coords=(575, 205))
                    keyboard.send_keys(fecha_hasta[0:2])

                    # Actualizar
                    time.sleep(1)
                    keyboard.send_keys('{F5}')
                    vertical_scroll = self.sscc.main.child_window(
                        title="Vertical", auto_id="NonClientVerticalScrollBar", control_type="ScrollBar",  found_index=0
                    ).wait('exists enabled visible ready', timeout=120)

                    # Exportar
                    keyboard.send_keys('{F7}')
                    btn_accept = self.sscc.main.child_window(title="Aceptar", auto_id="9", control_type="Button").wait('exists enabled visible ready')
                    btn_accept.click()
                    time.sleep(5)
                    export_dlg_handles = findwindows.find_windows(title='Exportar')
                    if export_dlg_handles:
                        export_dlg = self.sscc.app.window_(handle=export_dlg_handles[0])

                    btn_escritorio = export_dlg.child_window(title="Escritorio", control_type="TreeItem", found_index=1).wrapper_object()
                    btn_escritorio.click_input()

                    cmb_tipo = export_dlg.child_window(title="Tipo:", auto_id="FileTypeControlHost", control_type="ComboBox").wrapper_object()
                    cmb_tipo.type_keys("%{DOWN}")
                    cmb_tipo.select('Archivo ASCII separado por comas (*.csv)')

                    cmb_nombre = export_dlg.child_window(title="Nombre:", auto_id="FileNameControlHost", control_type="ComboBox").wrapper_object()
                    cmb_nombre.click_input()
                    report_name = ejercicio + ' - Bancos - Consulta General de Movimientos.csv'
                    cmb_nombre.type_keys(report_name,  with_spaces=True)
                    btn_guardar = export_dlg.child_window(title="Guardar", auto_id="1", control_type="Button").wrapper_object()
                    btn_guardar.click()

                    dlg_consulta_gral_mov = self.sscc.main.child_window(
                        title="Consulta General de Movimientos", control_type="Window"
                    ).wait('active', timeout=60)

                    # Cerrar ventana
                    keyboard.send_keys('{F10}')

                    # Move file to destination
                    self.sscc.move_report(
                        dir_path, report_name
                    )

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.sscc.quit()

    # --------------------------------------------------
    def from_external_report(self, csv_path:str) -> pd.DataFrame:
        """"Read from csv SSCC's report"""
        df = self.read_csv(csv_path)
        read_title = df['1'].iloc[0]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read csv file"""
        df = self.df
        df = df.replace(to_replace='[\r\n]', value='')
        df = df >> \
            dplyr.transmute(
                fecha = f['20'],
                ejercicio = f.fecha.str[-4:],
                mes = f.fecha.str[3:5] + '/' + f.ejercicio,
                cta_cte = f['22'],
                movimiento = base.trimws(f['21']),
                es_cheque = dplyr.case_when(
                    (f.movimiento == "DEBITO") | (f.movimiento == "DEPOSITO"), False,
                    True, True
                ),
                concepto = f['23'],
                beneficiario = f['24'],
                moneda = f['25'],
                libramiento = f['26'],
                imputacion = f['27'],
                importe = base.as_double(
                    base.gsub(',', '', f['28']))
            ) >> \
            tidyr.separate(
                f.imputacion, 
                into = ['cod_imputacion', 'imputacion'], 
                sep= '-' ,remove=True, extra='merge'
            ) >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha,
                f.cta_cte, f.movimiento, f.es_cheque,
                f.beneficiario, f.importe,
                dplyr.everything()
            )

        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%d/%m/%Y'
        )

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SSCC's 'Consulta " +
        "General de Movimientos' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='',
        type=str,
        help = "SSCC' Consulta General de Movimientos.csv report. Must be in the same folder")

    parser.add_argument('--download', action='store_true')
    parser.add_argument('--no-download', dest='download', action='store_false')
    parser.set_defaults(download=True)

    parser.add_argument(
        '-u', '--username', 
        metavar = 'Username',
        default = '',
        type=str,
        help = "Username to log in SSCC")

    parser.add_argument(
        '-p', '--password', 
        metavar = 'Password',
        default = '',
        type=str,
        help = "Password to log in SSCC")

    parser.add_argument(
        '-e', '--ejercicio', 
        metavar = 'Ejercicio',
        default = '2023',
        type=str,
        help = "Ejercicio to download from SSCC")

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
        json_path = dir_path + '/sscc_credentials.json'
        if args.username != '' and args.password != '':
            sscc_connection = ConnectSSCC(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    sscc_connection = ConnectSSCC(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        sscc_banco_invico = BancoINVICO(sscc = sscc_connection)
        sscc_banco_invico.download_report(
            dir_path, ejercicios=args.ejercicio
        )
        sscc_connection.quit()
    else:
        sscc_banco_invico = BancoINVICO()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + ' - Bancos - Consulta General de Movimientos.csv'

    sscc_banco_invico.from_external_report(dir_path + '/' + filename)
    sscc_banco_invico.to_sql(dir_path + '/sscc.sqlite')
    sscc_banco_invico.print_tidyverse()
    sscc_banco_invico.from_sql(dir_path + '/sscc.sqlite')
    sscc_banco_invico.print_tidyverse()

    sscc_banco_invico.test_sql(dir_path + '/test.sqlite')
    print(sscc_banco_invico.df.head(10))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sscc.banco_invico -e 2022
