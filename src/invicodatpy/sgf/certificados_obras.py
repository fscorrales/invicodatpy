#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Read, process and write SGF's 'Informe para Contable' report
"""

__all__ = ['CertificadosObras']

import argparse
import inspect
import os

import pandas as pd

from ..models.sgf_model import SGFModel
from ..utils.rpw_utils import RPWUtils


class CertificadosObras(RPWUtils):
    """Read, process and write SGF's 'Informe para Contable' report"""
    _REPORT_TITLE = 'Resumen de Certificaciones: '
    _TABLE_NAME = 'certificados_obras'
    _INDEX_COL = 'id'
    _FILTER_COL = 'ejercicio'
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
        condition = (df['37'] == "TOTALES") | (df['48'] == "TOTALES")
        df = df.assign(
            ejercicio = df['2'].iloc[0][-4:],
            beneficiario = df['21'].where(condition, None),
            obra = df['22'].where(condition, df['21']),
            nro_certificado = df['23'].where(condition, df['22']),
            monto_certificado = df['26'].where(condition, df['25']),
            fondo_reparo = df['27'].where(condition, df['26']),
            otros = df['28'].where(condition, df['27']),
            importe_bruto = df['29'].where(condition, df['28']),
            iibb = df['30'].where(condition, df['29']),
            lp = df['31'].where(condition, df['30']),
            suss = df['32'].where(condition, df['31']),
            gcias = df['33'].where(condition, df['32']),
            invico = df['34'].where(condition, df['33']),
            retenciones = df['35'].where(condition, df['34']),
            importe_neto = df['36'].where(condition, df['35']),
        )
        df['beneficiario'] = df['beneficiario'].fillna(method='ffill')
        to_numeric_cols = [
            'monto_certificado', 'fondo_reparo', 'otros',
            'importe_bruto', 'iibb', 'lp',
            'suss', 'gcias', 'invico',
            'retenciones', 'importe_neto',
        ]
        df[to_numeric_cols] = df[to_numeric_cols].apply(
            lambda x: x.str.replace(',', '').astype(float))
        df[['cod_obra', '_']] = df['obra'].str.split(
            pat = '-', n=1, expand=True
        )
        df = df.loc[:, [
            'ejercicio', 'beneficiario', 'cod_obra', 'obra',
            'nro_certificado', 'monto_certificado', 'fondo_reparo',
            'otros', 'importe_bruto', 'iibb', 'lp', 'suss', 'gcias',
            'invico', 'retenciones', 'importe_neto'
        ]]

        self.df = df
        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SGF's 'Informe para Contable' report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "csv_file",
        default='2022 Informe para Contable.csv',
        type=str,
        help = "SGF' Informe para Contable.csv report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    sgf_informe_contable = CertificadosObras()
    sgf_informe_contable.from_external_report(dir_path + '/' + args.file)
    # sgf_informe_contable.test_sql(dir_path + '/test.sqlite')
    sgf_informe_contable.to_sql(dir_path + '/sgf.sqlite')
    sgf_informe_contable.print_tidyverse()
    sgf_informe_contable.from_sql(dir_path + '/sgf.sqlite')
    sgf_informe_contable.print_tidyverse()
    # print(siif_rf610.df.head(10))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgf.certificados_obras
