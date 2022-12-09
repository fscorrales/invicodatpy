#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Read, process and write SIIF's rf610 report
"""

import argparse
import inspect
import os

import pandas as pd
from datar import base, dplyr, tidyr,f
from .siif import SIIF

class PptoGtosDescRf610(SIIF):
    """Read, process and write SIIF's rf610 report"""
    _REPORT_TITLE = 'LISTADO DE EJECUCION DE GASTOS POR PARTIDA'
    _TABLE_NAME = 'ppto_gtos_desc_rf610'
    _INDEX_COL = 'id'
    _FILTER_COL = 'ejercicio'

    # --------------------------------------------------
    def read_siif_report(self, xls_path:str) -> pd.DataFrame:
        """"Read from xls SIIF's report"""
        df = self.read_xls(xls_path)
        read_title = df['32'].iloc[2] + ' ' + df['32'].iloc[4]
        if read_title == self._REPORT_TITLE:
            self.df = df
            self.transform_df()
        else:
            # Future exception raise
            pass

    # --------------------------------------------------
    def transform_df(self) -> pd.DataFrame:
        """"Transform read xls file"""
        self.df = self.df.replace(to_replace='', value=None)
        self.df = self.df >> \
            dplyr.transmute(
                ejercicio = f['33'].iloc[9][-4:],
                programa = f['5'],
                subprograma=  f['9'],
                proyecto =  f['14'],
                actividad =  f['17'],
                grupo = f['20'],
                partida = f['21'],
                desc_part = f['24'],
                credito_original = f['38'],
                credito_vigente =  f['44'],
                comprometido = f['49'],
                ordenado = f['55'],
                saldo = f['60']
            ) >> \
            base.tail(-30) >> \
            tidyr.fill(
                f.programa, f.subprograma,
                f.proyecto, f.actividad, f.grupo, 
                f.partida, f.desc_part) >> \
            tidyr.drop_na(f.credito_original) >> \
            tidyr.separate(
                f.programa, 
                into = ['programa', 'desc_prog'], 
                sep= ' ' ,remove=True, extra='merge'
            ) >> \
            tidyr.separate(
                f.subprograma, 
                into = ['subprograma', 'desc_subprog'], 
                sep= ' ' ,remove=True, extra='merge'
            ) >> \
            tidyr.separate(
                f.proyecto, 
                into = ['proyecto', 'desc_proy'], 
                sep= ' ' ,remove=True, extra='merge'
            ) >> \
            tidyr.separate(
                f.actividad, 
                into = ['actividad', 'desc_act'], 
                sep= ' ' ,remove=True, extra='merge'
            ) >> \
            tidyr.separate(
                f.grupo, 
                into = ['grupo', 'desc_gpo'], 
                sep= ' ' ,remove=True, extra='merge'
            ) >> \
            dplyr.mutate(
                programa = f.programa.str.zfill(2),
                subprograma = f.subprograma.str.zfill(2),
                proyecto = f.proyecto.str.zfill(2),
                actividad = f.actividad.str.zfill(2),
                desc_prog = base.trimws(f.desc_prog),
                desc_subprog = base.trimws(f.desc_subprog),
                desc_proy = base.trimws(f.desc_proy),
                desc_act = base.trimws(f.desc_act),
                desc_gpo = base.trimws(f.desc_gpo),
                credito_original = base.as_double(f.credito_original),
                credito_vigente = base.as_double(f.credito_vigente),
                comprometido = base.as_double(f.comprometido),
                ordenado = base.as_double(f.ordenado),
                saldo = base.as_double(f.saldo)
            ) >> \
            tidyr.unite(
                'estructura',
                [f.programa, f.subprograma, f.proyecto,
                f.actividad, f.partida],
                sep='-', remove=False
            ) >> \
            dplyr.select(
                f.ejercicio, f.estructura,
                f.programa, f.desc_prog, 
                f.subprograma, f.desc_subprog, 
                f.proyecto, f.desc_proy, 
                f.actividad, f.desc_act,
                f.grupo, f.desc_gpo,
                dplyr.everything()
            )

        return self.df

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Read, process and write SIIF's rf610 report",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-f', '--file', 
        metavar = "xls_file",
        default='2022-rf610.xls',
        type=str,
        help = "SIIF' rf610.xls report. Must be in the same folder")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    siif_rf610 = PptoGtosDescRf610()
    siif_rf610.read_siif_report(dir_path + '/' + args.file)
    siif_rf610.to_sql(dir_path + '/siif.sqlite')
    siif_rf610.print_tidyverse()
    # df = siif_rf610.from_sql(dir_path + '/siif.sqlite')
    # siif_rf610.print_tidyverse()
    # siif_rf610.test_sql(dir_path + '/test.sqlite', siif_rf610.df)
    # print(siif_rf610.df.head(10))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.siif.ppto_gtos_desc_rf610 -f 2022-rf610.xls
