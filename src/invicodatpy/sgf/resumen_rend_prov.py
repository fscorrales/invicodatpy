#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SGF's 'Resumen de 
Rendiciones por Proveedores' report
"""

import argparse
import datetime as dt
import inspect
import json
import os
import time
from dataclasses import dataclass, field

import pandas as pd
from datar import dplyr, f, base
from pywinauto import findwindows, keyboard, mouse

from ..models.sgf_model import SGFModel
from ..utils.rpw_utils import RPWUtils
from .connect_sgf import ConnectSGF

@dataclass
class ResumenRendProv(RPWUtils):
    """
    Read, process and write SGF's 'Resumen de Rendiciones 
    por Proveedores' report
    :param sgf_connection must be initialized first in order to automate SSCC
    """
    _REPORT_TITLE:str = field(
        init=False, repr=False, 
        default='Resumen de Rendiciones (Detalle)'
    )
    _TABLE_NAME:str = field(
        init=False, repr=False, 
        default='resumen_rend_prov'
    )
    _INDEX_COL:str = field(
        init=False, repr=False, default='id'
    )
    _FILTER_COL:str = field(
        init=False, repr=False, 
        default_factory=lambda:['origen', 'mes']
    )
    _SQL_MODEL:SGFModel = field(
        init=False, repr=False, default=SGFModel
    )
    sgf:ConnectSGF = field(
        init=True, repr=False, default=None
    )

    # --------------------------------------------------
    def download_report(
        self, dir_path:str, 
        ejercicios:list = str(dt.datetime.now().year),
        origenes:list = ['EPAM', 'OBRAS', 'FUNCIONAMIENTO']
    ):
        try:
            if not isinstance(origenes, list):
                origenes = [origenes]
            if not isinstance(ejercicios, list):
                ejercicios = [ejercicios]
            for origen in origenes:
                for ejercicio in ejercicios:
                    # Open menu Consulta General de Movimientos
                    self.sgf.main.menu_select("Informes->Resumen de Rendiciones")

                    dlg_resumen_rend = self.sgf.main.child_window(
                        title="Informes - Resumen de Rendiciones", control_type="Window"
                    ).wait('exists')

                    int_ejercicio = int(ejercicio)
                    if int_ejercicio > 2010 and int_ejercicio <= dt.datetime.now().year:
                        # Origen
                        cmb_origen = self.sgf.main.child_window(auto_id="24", control_type="ComboBox").wrapper_object()
                        cmb_origen.type_keys("%{DOWN}")
                        cmb_origen.type_keys(origen,  with_spaces=True) #EPAM, OBRAS, FUNCIONAMIENTO
                        keyboard.send_keys('{ENTER}')
                        btn_exportar = self.sgf.main.child_window(
                            title="Exportar", auto_id="4", control_type="Button"
                        ).wait('enabled ready active', timeout=60)
                        
                        # Fecha Desde
                        ## Click on año desde
                        time.sleep(1)
                        mouse.click(coords=(205, 415))
                        keyboard.send_keys(ejercicio)
                        ## Click on mes desde
                        time.sleep(1)
                        mouse.click(coords=(185, 415))
                        keyboard.send_keys('01')
                        ## Click on día desde
                        time.sleep(1)
                        mouse.click(coords=(170, 415))
                        keyboard.send_keys('01')

                        # Fecha Hasta
                        fecha_hasta = dt.datetime(year=(int_ejercicio), month=12, day=31)
                        fecha_hasta = min(fecha_hasta, dt.datetime.now())
                        fecha_hasta = dt.datetime.strftime(fecha_hasta, '%d/%m/%Y')
                        ## Click on año hasta
                        time.sleep(1)
                        mouse.click(coords=(495, 415))
                        keyboard.send_keys(ejercicio)
                        ## Click on mes hasta
                        time.sleep(1)
                        mouse.click(coords=(470, 415))
                        keyboard.send_keys(fecha_hasta[3:5])
                        ## Click on día hasta
                        time.sleep(1)
                        mouse.click(coords=(455, 415))
                        keyboard.send_keys(fecha_hasta[0:2])

                        # Exportar
                        btn_exportar.click()
                        btn_accept = self.sgf.main.child_window(
                            title="Aceptar", auto_id="9", control_type="Button"
                        ).wait('exists enabled visible ready', timeout=360)
                        btn_accept.click()
                        time.sleep(5)
                        export_dlg_handles = findwindows.find_windows(title='Exportar')
                        if export_dlg_handles:
                            export_dlg = self.sgf.app.window_(handle=export_dlg_handles[0])

                        btn_escritorio = export_dlg.child_window(
                            title="Escritorio", control_type="TreeItem", found_index=1
                        ).wrapper_object()
                        btn_escritorio.click_input()

                        cmb_tipo = export_dlg.child_window(
                            title="Tipo:", auto_id="FileTypeControlHost", control_type="ComboBox"
                        ).wrapper_object()
                        cmb_tipo.type_keys("%{DOWN}")
                        cmb_tipo.select('Archivo ASCII separado por comas (*.csv)')

                        cmb_nombre = export_dlg.child_window(
                            title="Nombre:", auto_id="FileNameControlHost", control_type="ComboBox"
                        ).wrapper_object()
                        cmb_nombre.click_input()
                        report_name = ejercicio + ' Resumen de Rendiciones '+ origen +'.csv'
                        cmb_nombre.type_keys(report_name,  with_spaces=True)
                        btn_guardar = export_dlg.child_window(
                            title="Guardar", auto_id="1", control_type="Button"
                        ).wrapper_object()
                        btn_guardar.click()

                        # dlg_resumen_rend = self.sgf.main.child_window(
                        #     title="Informes - Resumen de Rendiciones", control_type="Window"
                        # ).wait_not('visible exists', timeout=120)

                        self.sgf.main.wait("active", timeout=120)

                        # Move file to destination
                        time.sleep(2)
                        self.sgf.move_report(
                            dir_path, report_name
                        )

        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.sgf.quit()

    # --------------------------------------------------
    def from_external_report(self, csv_path:str) -> pd.DataFrame:
        """"Read from csv SGF's report"""
        df = self.read_csv(csv_path, names = list(range(0,70)))
        read_title = df['1'].iloc[0][0:32]
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
        df['origen'] = df['6'].str.split('-', n = 1).str[0]
        df['origen'] = df['origen'].str.split('=', n = 1).str[1]
        df['origen'] = df['origen'].str.replace('"','')
        df['origen'] = df['origen'].str.strip()
        
        if df.loc[0, 'origen'] == 'OBRAS':
            df = df >> \
                dplyr.transmute(
                    origen = f.origen,
                    beneficiario = f['23'], 
                    destino = '', 
                    libramiento_sgf = f['25'], 
                    fecha = f['26'], 
                    movimiento = f['27'],
                    cta_cte = f['24'],
                    importe_bruto = f['28'],
                    gcias = f['29'], 
                    sellos = f['30'],
                    iibb = f['31'], 
                    suss = f['32'], 
                    invico = f['33'], 
                    seguro = '0',
                    salud = '0', 
                    mutual = '0', 
                    otras = f['34'],
                    importe_neto = f['35'], 
                )
        else:
            df = df >> \
                dplyr.transmute(
                    origen = f.origen,
                    beneficiario = f['26'], 
                    destino = f['27'], 
                    libramiento_sgf = f['29'], 
                    fecha = f['30'], 
                    movimiento = f['31'],
                    cta_cte = f['28'],
                    importe_bruto = f['41'], 
                    gcias = f['33'], 
                    sellos = f['34'],
                    iibb = f['35'], 
                    suss = f['36'], 
                    invico = f['37'], 
                    seguro = f['38'],
                    salud = f['39'], 
                    mutual = f['40'], 
                    otras = '0',
                    importe_neto = f['32']
                )
        
        df.loc[:,'importe_bruto':] = df.loc[:,'importe_bruto':].stack(
        ).str.replace(',','').unstack()
        df.loc[:,'importe_bruto':] = df.loc[:,'importe_bruto':].stack(
        ).astype(float).unstack()
        df['retenciones'] = df.loc[:,'gcias':'otras'].sum(axis=1)

        df = df >> \
            dplyr.mutate(
                importe_bruto = dplyr.if_else( #SGF no suma 3% en bruto
                    f.origen == 'EPAM', 
                    f.importe_bruto + f.invico,
                    f.importe_bruto
                )
            ) >> \
            dplyr.relocate(f.retenciones, _before = f.importe_neto) >> \
            dplyr.mutate(
                ejercicio = f.fecha.str[-4:],
                mes = f.fecha.str[3:5] + '/' + f.ejercicio,
                cta_cte = dplyr.if_else(f.beneficiario == 'CREDITO ESPECIAL',
                                        "130832-07", f.cta_cte),
            )  >> \
            dplyr.select(
                f.origen, f.ejercicio, f.mes, f.fecha,
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
        description = "Read, process and write SGF's 'Resumen de " +
        "Rendiciones por Proveedores' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='',
        type=str,
        help = "SGF' Resumen de Rendiciones.csv report. Must be in the same folder")

    parser.add_argument('--download', action='store_true')
    parser.add_argument('--no-download', dest='download', action='store_false')
    parser.set_defaults(download=True)

    parser.add_argument(
        '-u', '--username', 
        metavar = 'Username',
        default = '',
        type=str,
        help = "Username to log in SGF")

    parser.add_argument(
        '-p', '--password', 
        metavar = 'Password',
        default = '',
        type=str,
        help = "Password to log in SGF")

    parser.add_argument(
        '-e', '--ejercicio', 
        metavar = 'Ejercicio',
        default = '2023',
        type=str,
        help = "Ejercicio to download from SGF")

    parser.add_argument(
        '-o', '--origen', 
        metavar = 'Origen',
        default = '',
        type=str,
        help = "Origen to download from SGF")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))

    if args.origen == '':
        origenes = ['EPAM', 'OBRAS', 'FUNCIONAMIENTO']
    else:
        origenes = [args.origen]

    if args.download:
        json_path = dir_path + '/sgf_credentials.json'
        if args.username != '' and args.password != '':
            sgf_connection = ConnectSGF(args.username, args.password)
        else:
            if os.path.isfile(json_path):
                with open(json_path) as json_file:
                    data_json = json.load(json_file)
                    sgf_connection = ConnectSGF(
                        data_json['username'], data_json['password']
                    )
                json_file.close()
        sgf_resumen_rend_prov = ResumenRendProv(sgf = sgf_connection)
        sgf_resumen_rend_prov.download_report(
            dir_path, ejercicios=args.ejercicio, origenes=origenes
        )
        sgf_connection.quit()
    else:
        sgf_resumen_rend_prov = ResumenRendProv()

    if args.file != '':
        filename = args.file
    else:
        filename = args.ejercicio + ' Resumen de Rendiciones '+ origenes[0] + '.csv'

    sgf_resumen_rend_prov.from_external_report(dir_path + '/' + filename)
    # sgf_resumen_rend_prov.test_sql(dir_path + '/test.sqlite')
    sgf_resumen_rend_prov.to_sql(dir_path + '/sgf.sqlite')
    sgf_resumen_rend_prov.print_tidyverse()
    sgf_resumen_rend_prov.from_sql(dir_path + '/sgf.sqlite')
    sgf_resumen_rend_prov.print_tidyverse()
    # print(sgf_resumen_rend_prov.df.head(10))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgf.resumen_rend_prov -o FUNCIONAMIENTO
