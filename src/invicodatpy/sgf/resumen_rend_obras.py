#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SGF's 'Resumen de 
Rendiciones por Obra' report
"""

__all__ = ['ResumenRendObras']

import argparse
import inspect
import os

import pandas as pd

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
        df = df.assign(
            obra = df['obra'],
            beneficiario = df['25'].where(df['55'] == '', df['36']),
            libramiento_sgf = df['26'].where(df['55'] == '', df['37']),
            destino = df['27'].where(df['55'] == '', df['38']),
            fecha = df['28'].where(df['55'] == '', df['39']),
            movimiento = df['29'].where(df['55'] == '', df['40']),
            importe_bruto = df['39'].where(df['55'] == '', df['50']),
            gcias = df['31'].where(df['55'] == '', df['42']),
            sellos = df['32'].where(df['55'] == '', df['43']),
            lp = df['33'].where(df['55'] == '', df['44']),
            iibb = df['34'].where(df['55'] == '', df['45']),
            suss = df['35'].where(df['55'] == '', df['46']),
            seguro = df['36'].where(df['55'] == '', df['47']),
            salud = df['37'].where(df['55'] == '', df['48']),
            mutual = df['38'].where(df['55'] == '', df['49']),
            retenciones = '0',
            importe_neto = df['30'].where(df['55'] == '', df['41']),
        )
        df['ejercicio'] = df['fecha'].str[-4:]
        df['mes'] = df['fecha'].str[3:5] + '/' + df['ejercicio']
        df[['cod_obra', '_']] = df['obra'].str.split(pat = '-', n=1, expand=True)
        df['fecha'] = pd.to_datetime(
            df['fecha'], format='%d/%m/%Y'
        )
        df = df.replace(to_replace='', value='0')
        df['cod_obra'] = df['cod_obra'].str.strip()
        to_numeric_cols = [
            'importe_bruto', 'gcias', 'sellos', 'lp', 'iibb', 'suss',
            'seguro', 'salud', 'mutual', 'importe_neto'
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(
            lambda x: x.str.replace(',', '').astype(float))
        cols_to_sum = [col for col in to_numeric_cols if col not in [
            'importe_neto', 'importe_bruto'
        ]]
        df['retenciones'] = df[cols_to_sum].sum(axis=1)
        df = df.loc[:, [
            'ejercicio', 'mes', 'fecha', 'beneficiario', 
            'cod_obra', 'obra', 'destino', 'libramiento_sgf',
            'libramiento_sgf', 'movimiento', 'importe_bruto', 'gcias',
            'sellos', 'lp', 'iibb', 'suss', 'seguro', 'salud', 'mutual',
            'retenciones', 'importe_neto'
        ]]

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
    sgf_resumen_rend_obras.test_sql(dir_path + '/test.sqlite')
    # sgf_resumen_rend_obras.to_sql(dir_path + '/sgf.sqlite')
    # sgf_resumen_rend_obras.print_tidyverse()
    # sgf_resumen_rend_obras.from_sql(dir_path + '/sgf.sqlite')
    # sgf_resumen_rend_obras.print_tidyverse()
    # print(sgf_resumen_rend_obras.df.head(10))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgf.resumen_rend_obras
