#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Migrate from slave.mdb to slave.sqlite
Linux system read mdb requirements: 
    details: csv exported from .mdb with MDBTOOLS (sudo apt install mdbtools).
    example: 
        mdb-export Slave.mdb PRECARIZADOS -d '\t' > precarizados.csv
        mdb-export Slave.mdb LIQUIDACIONHONORARIOS -d '\t' > honorarios.csv
    MDBTOOLS info: https://github.com/mdbtools/mdbtools
"""

import argparse
import csv  # https://www.pythontutorial.net/python-basics/python-write-csv-file/
import datetime as dt
import inspect
import os
import subprocess  # https://www.section.io/engineering-education/how-to-execute-linux-commands-in-python/
import sys
from dataclasses import dataclass

import pandas as pd

from ..models.slave_model import SlaveModel
from ..utils.rpw_utils import RPWUtils


@dataclass
class MigrateSlave(RPWUtils):
    """Migrate from slave.mdb to slave.sqlite"""
    path_old_slave:str = 'Slave.mdb'
    path_new_slave:str = 'slave.sqlite'
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
    # def connect_mdb(self, file:str, table_name:str):
    # """Inicializa la conexión a la BD de slave y refleja las tablas de la misma"""
    # parser = argparse.ArgumentParser(description='Convert slave.mdb file to .sqlite')
    # parser.add_argument('-f', '--file', metavar='Name', default='Slave.mdb', help='.mdb File to convert')
    # args = parser.parse_args()
    # metadata = MetaData()
    # connection_string = (
    #     r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    #     r"DBQ=D:\Datos INVICO\Python INVICO\Lectura archivos\read_mdb"
    #     + "\\" + args.file +
    #     r";"
    #     r"ExtendedAnsiSQL=1;"
    # )

    # --------------------------------------------------
    def migrate_all(self):
        self.migrate_factureros()
        self.migrate_honorarios_factureros()

    # --------------------------------------------------
    def migrate_factureros(self) -> pd.DataFrame:
        """"Migrate table PRECARIZADOS"""
        if sys.platform.startswith('linux'):
            csv_output = os.path.join(
                os.path.dirname(self.path_old_slave),
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
        elif sys.platform.startswith('win32'):
            self._TABLE_NAME = 'PRECARIZADOS'
            df = self.from_mdb(self.path_old_slave)
            df.rename(columns={
                "Agentes":"razon_social",
                "Actividad":"actividad",
                "Partida":"partida"
                }, inplace=True)
        df.drop_duplicates(inplace=True)
        df['actividad'] = df['actividad'].str[0:3] + '00-' + df['actividad'].str[3:]
        self._TABLE_NAME = 'factureros'
        self.df = df
        self.to_sql(self.path_new_slave, True)

    # --------------------------------------------------
    def migrate_honorarios_factureros(self) -> pd.DataFrame:
        """"Migrate table HONORARIOS"""
        if sys.platform.startswith('linux'):
            csv_output = os.path.join(
                os.path.dirname(self.path_old_slave),
                'honorarios_factureros.csv'
            )
            self.mdbtools_export(
                self.path_old_slave, 'LIQUIDACIONHONORARIOS', csv_output)
            df = self.read_csv(csv_output, header=0)
            df.rename(columns={
                    "0":"fecha",
                    "1":"razon_social",
                    "2":"sellos",
                    "3":"seguro",
                    "5":"tipo",
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
            df['fecha'] = pd.to_datetime(
                df['fecha'], format='%m/%d/%y %H:%M:%S'
            )
        elif sys.platform.startswith('win32'):
            self._TABLE_NAME = 'LIQUIDACIONHONORARIOS'
            df = self.from_mdb(self.path_old_slave)
            df.rename(columns={
                    "Fecha":"fecha",
                    "Proveedor":"razon_social",
                    "Sellos":"sellos",
                    "Seguro":"seguro",
                    "Tipo":"tipo",
                    "Comprobante":"nro_comprobante",
                    "MontoBruto":"importe_bruto",
                    "IIBB":"iibb",
                    "LibramientoPago":"lp",
                    "OtraRetencion":"otras_retenciones",
                    "Anticipo":"anticipo",
                    "Descuento":"descuento",
                    "Actividad":"actividad",
                    "Partida":"partida",
                }, inplace=True)

        # Table honorarios_factureros
        df['ejercicio'] = df['fecha'].dt.year.astype(str)
        df['mes'] = df['fecha'].dt.strftime('%m/%Y')
        df['mutual'] = 0
        df['embargo'] = 0
        keep = ['NoSIIF']
        df = df.loc[~df.nro_comprobante.str.contains('|'.join(keep))]
        self._TABLE_NAME = 'honorarios_factureros'
        self.df = df
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

    migrate_slave = MigrateSlave(os.path.join(dir_path, args.old_slave), 
                                os.path.join(dir_path, args.new_slave))
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
