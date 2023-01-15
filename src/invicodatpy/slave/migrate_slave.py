#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Migrate from slave.mdb to slave.sqlite
csv files creation: 
    details: exported from .mdb with MDBTOOLS (sudo apt install mdbtools).
    example: 
        mdb-export Slave.mdb PRECARIZADOS -d '|' > precarizados.csv
        mdb-export Slave.mdb LIQUIDACIONHONORARIOS -d '|' > honorarios.csv
    MDBTOOLS info: https://github.com/mdbtools/mdbtools
"""

import argparse
import csv #https://www.pythontutorial.net/python-basics/python-write-csv-file/
import datetime as dt
import inspect
import os
import subprocess #https://www.section.io/engineering-education/how-to-execute-linux-commands-in-python/
from dataclasses import dataclass

import pandas as pd

from ..models.slave_model import SlaveModel
from ..utils.rpw_utils import RPWUtils


@dataclass
class MigrateSlave(RPWUtils):
    """Migrate from slave.mdb to slave.sqlite"""
    path_old_slave:str = 'Slave.mdb'
    path_new_slave:str = 'slave.sqlite'
    use_mdbtools:bool = True
    _SQL_MODEL = SlaveModel
    _INDEX_COL = None

    # --------------------------------------------------
    def mdbtools_tables(self, file:str):
        # The command you want to execute   
        cmd = 'mdb-tables'
        temp = subprocess.Popen([cmd, file], stdout = subprocess.PIPE) 
        # get the output as a string
        output = temp.communicate()[0].decode()
        return output

    # --------------------------------------------------
    def mdbtools_export(
        self, file:str, table_name:str, output_name:str):
        # The command you want to execute 
        cmd = 'mdb-export'
        temp = subprocess.Popen([cmd, file, table_name, '--delimiter=\t'], stdout = subprocess.PIPE) 
        # get the output as a list of lists
        output = temp.communicate()[0].decode()
        output = output.replace('"', '')
        output = [el.split("\t") for el in output.splitlines()]
        with open(output_name, 'w', encoding="ISO-8859-1") as file:
            writer = csv.writer(file, quoting = csv.QUOTE_ALL)
            writer.writerows(output)

    # --------------------------------------------------
    def migrate_all(self):
        self.migrate_factureros()
        self.migrate_honorarios_factureros()

    # --------------------------------------------------
    def migrate_factureros(self) -> pd.DataFrame:
        """"Migrate table PRECARIZADOS"""
        if self.use_mdbtools:
            csv_output = os.path.join(
                os.path.dirname(self.path_new_slave),
                'precarizados.csv'
            )
            self.mdbtools_export(
                self.path_old_slave, 'PRECARIZADOS', csv_output)
            df = self.read_csv(csv_output, header=0)
        df.rename(columns={
            "0":"razon_social",
            "1":"actividad",
            "2":"partida"
            }, inplace=True)
        df.drop_duplicates(inplace=True)
        df['actividad'] = df['actividad'].str[0:3] + '00-' + df['actividad'].str[3:]
        self._TABLE_NAME = 'factureros'
        self.df = df
        self.to_sql(self.path_new_slave, True)

    # --------------------------------------------------
    def migrate_honorarios_factureros(self) -> pd.DataFrame:
        """"Migrate table HONORARIOS"""
        if self.use_mdbtools:
            csv_output = os.path.join(
                os.path.dirname(self.path_new_slave),
                'honorarios_factureros.csv'
            )
            self.mdbtools_export(
                self.path_old_slave, 'LIQUIDACIONHONORARIOS', csv_output)
            df = self.read_csv(csv_output, header=0)
        # Table comprobantes_siif
        siif = df.loc[:,['4', '0', '5']]
        siif.rename(columns={
            "4":"nro_comprobante",
            "0":"fecha",
            "5":"tipo"
            }, inplace=True)
        siif.drop_duplicates(inplace=True)
        self._TABLE_NAME = 'comprobantes_siif'
        self.df = siif
        self.to_sql(self.path_new_slave, True)
        # Table honorarios_factureros
        honorarios = df.drop(['0', '5'],axis=1)
        honorarios.rename(columns={
            "1":"razon_social",
            "2":"sellos",
            "3":"seguro",
            "4":"nro_comprobante",
            "6":"importe_bruto",
            "7":"iibb",
            "8":"lp",
            "9":"otras_retenciones",
            "10":"anticipo",
            "11":"descuento",
            "12":"actividad",
            "13":"partida",
            }, inplace=True)
        honorarios['mutual'] = 0
        honorarios['embargo'] = 0
        self._TABLE_NAME = 'honorarios_factureros'
        self.df = honorarios
        self.to_sql(self.path_new_slave, True)

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Migrate from slave.mdb to slave.sqlite",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-o', '--old_slave', 
        metavar = "old_slave",
        default='Slave.mdb',
        type=str,
        help = "Path to old Slave.mdb to be migrated")

    parser.add_argument(
        '-n', '--new_slave', 
        metavar = "new_slave",
        default='slave.sqlite',
        type=str,
        help = "Path to slave.sqlite")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))

    migrate_slave = MigrateSlave(dir_path +"/"+ args.old_slave, 
                                dir_path +"/"+ args.new_slave)
    migrate_slave.migrate_all()
    #df = migrate_slave.from_sql(dir_path +"/"+ args.new_icaro, 'carga')
    #print(df.head(5))
    # migrate_slave.migrate_honorarios_factureros()
    # migrate_slave.test_sql(dir_path + "/"+  'test.sqlite')

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.slave.migrate_slave
