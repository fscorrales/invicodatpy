#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SGF's 'Resumen de 
Rendiciones por Obra' report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, f, tidyr

from ..models.sgf_model import SGFModel
from ..utils.rpw_utils import RPWUtils


class ResumenRendObras(RPWUtils):
    """Read, process and write SGF's 'Resumen de Rendiciones por Obra' report"""
    _REPORT_TITLE = 'Resumen de Rendiciones (por Obras)'
    _TABLE_NAME = 'resumen_rend_obras'
    _INDEX_COL = 'id'
    _FILTER_COL = 'mes'
    _SQL_MODEL = SGFModel

    # --------------------------------------------------
    def from_external_report(self, csv_path:str) -> pd.DataFrame:
        """"Read from csv SGF's report"""
        df = self.read_csv(csv_path, names = list(range(0,70)))
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
        df.loc[df['55'] != '', 'obra'] = df['25']
        df.loc[df['obra'] == '', 'obra'] = df['38']
        df['obra'].fillna(method='ffill', inplace=True)
        df = df >> \
            dplyr.transmute(
                obra = f.obra,
                beneficiario = dplyr.case_when(
                    (f['55'] == ''), f['25'],
                    True, f['36']
                ),
                libramiento_sgf = dplyr.case_when(
                    (f['55'] == ''), f['26'],
                    True, f['37']
                ),
                destino = dplyr.case_when(
                    (f['55'] == ''), f['27'],
                    True, f['38']
                ),
                fecha = dplyr.case_when(
                    (f['55'] == ''), f['28'],
                    True, f['39']
                ),
                movimiento = dplyr.case_when(
                    (f['55'] == ''), f['29'],
                    True, f['40']
                ),
                importe_bruto = dplyr.case_when(
                    (f['55'] == ''), f['39'],
                    True, f['50']
                ),
                gcias = dplyr.case_when(
                    (f['55'] == ''), f['31'],
                    True, f['42']
                ),
                sellos = dplyr.case_when(
                    (f['55'] == ''), f['32'],
                    True, f['43']
                ),
                lp = dplyr.case_when(
                    (f['55'] == ''), f['33'],
                    True, f['44']
                ),
                iibb = dplyr.case_when(
                    (f['55'] == ''), f['34'],
                    True, f['45']
                ),
                suss = dplyr.case_when(
                    (f['55'] == ''), f['35'],
                    True, f['46']
                ),
                seguro = dplyr.case_when(
                    (f['55'] == ''), f['36'],
                    True, f['47']
                ),
                salud = dplyr.case_when(
                    (f['55'] == ''), f['37'],
                    True, f['48']
                ),
                mutual = dplyr.case_when(
                    (f['55'] == ''), f['38'],
                    True, f['49']
                ),
                retenciones = '0',
                importe_neto = dplyr.case_when(
                    (f['55'] == ''), f['30'],
                    True, f['41']
                ), 
                ejercicio = f.fecha.str[-4:],
                mes = f.fecha.str[3:5] + '/' + f.ejercicio
            ) >> \
            tidyr.separate(
                f.obra, 
                into = ['cod_obra', None], 
                sep= '-' ,remove=False, extra='merge'
            ) >> \
            dplyr.select(
                f.ejercicio, f.mes, f.fecha,
                f.beneficiario, f.cod_obra, f.obra, f.destino,
                dplyr.everything()
            )

        df = df.replace(to_replace='', value='0')

        df = df >> \
            dplyr.mutate(
                cod_obra = base.trimws(f.cod_obra),
                importe_bruto = base.gsub(',', '', f.importe_bruto),
                gcias = base.gsub(',', '', f.gcias),
                sellos = base.gsub(',', '', f.sellos),
                lp = base.gsub(',', '', f.lp),
                iibb = base.gsub(',', '', f.iibb),
                suss = base.gsub(',', '', f.suss),
                seguro = base.gsub(',', '', f.seguro),
                salud = base.gsub(',', '', f.salud),
                mutual = base.gsub(',', '', f.mutual),
                importe_neto = base.gsub(',', '', f.importe_neto)
            ) >> \
            dplyr.mutate(
                dplyr.across(base.c[f.importe_bruto:], base.as_double),
                retenciones = f.gcias + f.sellos + f.lp + 
                f.iibb + f.suss + f.seguro + f.salud + f.mutual
            ) >> \
            dplyr.relocate(f.retenciones, _before = f.importe_neto)

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
        "Rendiciones por Obra' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='2022 Resumen de Rendiciones EPAM por Obra.csv',
        type=str,
        help = "SGF' Resumen de Rendiciones.csv report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgf_resumen_rend_obras = ResumenRendObras()
    sgf_resumen_rend_obras.from_external_report(dir_path + '/' + args.file)
    # sgf_resumen_rend_obras.test_sql(dir_path + '/test.sqlite')
    sgf_resumen_rend_obras.to_sql(dir_path + '/sgf.sqlite')
    sgf_resumen_rend_obras.print_tidyverse()
    sgf_resumen_rend_obras.from_sql(dir_path + '/sgf.sqlite')
    sgf_resumen_rend_obras.print_tidyverse()
    # print(sgf_resumen_rend_obras.df.head(10))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgf.resumen_rend_obras
