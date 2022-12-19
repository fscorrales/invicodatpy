#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Migrate from old Icaro.sqlite to new DB
"""

import argparse
import datetime as dt
import inspect
import os
from dataclasses import dataclass

import pandas as pd

from ..models.icaro_model import IcaroModel
from ..utils.rpw_utils import RPWUtils


@dataclass
class MigrateIcaro(RPWUtils):
    """Migrate from old Icaro.sqlite to new DB"""
    path_old_icaro:str = 'ICARO.sqlite'
    path_new_icaro:str = 'icaro_new.sqlite'
    _SQL_MODEL = IcaroModel
    _INDEX_COL = None

    # --------------------------------------------------
    def migrate_all(self):
        self.migrate_programas()
        self.migrate_subprogramas()
        self.migrate_proyectos()
        self.migrate_actividades()
        self.migrate_ctas_ctes()
        self.migrate_fuentes()
        self.migrate_partidas()
        self.migrate_proveedores()
        self.migrate_obras()
        self.migrate_carga()
        self.migrate_retenciones()
        self.migrate_certificados_obras()
        self.migrate_resumen_rend_obras()

    # --------------------------------------------------
    def migrate_programas(self) -> pd.DataFrame:
        """"Migrate table programas"""
        self._TABLE_NAME = 'PROGRAMAS'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Programa":"programa",
            "DescProg":"desc_prog"
            }, inplace=True)
        self._TABLE_NAME = 'programas'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_subprogramas(self) -> pd.DataFrame:
        """"Migrate table subprogramas"""
        self._TABLE_NAME = 'SUBPROGRAMAS'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Programa":"programa",
            "DescSubprog":"desc_subprog",
            "Subprograma":"subprograma",
            }, inplace=True)
        self._TABLE_NAME = 'subprogramas'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_proyectos(self) -> pd.DataFrame:
        """"Migrate table proyectos"""
        self._TABLE_NAME = 'PROYECTOS'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Proyecto":"proyecto",
            "DescProy":"desc_proy",
            "Subprograma":"subprograma",
            }, inplace=True)
        self._TABLE_NAME = 'proyectos'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_actividades(self) -> pd.DataFrame:
        """"Migrate table actividades"""
        self._TABLE_NAME = 'ACTIVIDADES'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Actividad":"actividad",
            "DescAct":"desc_act",
            "Proyecto":"proyecto",
            }, inplace=True)
        self._TABLE_NAME = 'actividades'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_ctas_ctes(self) -> pd.DataFrame:
        """"Migrate table ctas_ctes"""
        self._TABLE_NAME = 'CUENTASBANCARIAS'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "CuentaAnterior":"cta_cte_anterior",
            "Cuenta":"cta_cte",
            "Descripcion":"desc_cta_cte",
            "Banco":"banco",
            }, inplace=True)
        self._TABLE_NAME = 'ctas_ctes'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_fuentes(self) -> pd.DataFrame:
        """"Migrate table fuentes"""
        self._TABLE_NAME = 'FUENTES'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Fuente":"fuente",
            "Descripcion":"desc_fte",
            "Abreviatura":"abreviatura",
            }, inplace=True)
        self._TABLE_NAME = 'fuentes'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_partidas(self) -> pd.DataFrame:
        """"Migrate table partidas"""
        self._TABLE_NAME = 'PARTIDAS'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Grupo":"grupo",
            "DescGrupo":"desc_grupo",
            "PartidaParcial":"partida_parcial",
            "DescPartidaParcial":"desc_part_parcial",
            "Partida":"partida",
            "DescPartida":"desc_part",
            }, inplace=True)
        self._TABLE_NAME = 'partidas'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_proveedores(self) -> pd.DataFrame:
        """"Migrate table proveedores"""
        self._TABLE_NAME = 'PROVEEDORES'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Codigo":"codigo",
            "Descripcion":"desc_prov",
            "Domicilio":"domicilio",
            "Localidad":"localidad",
            "Telefono":"telefono",
            "CUIT":"cuit",
            "CondicionIVA":"condicion_iva",
            }, inplace=True)
        self._TABLE_NAME = 'proveedores'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_obras(self) -> pd.DataFrame:
        """"Migrate table obras"""
        self._TABLE_NAME = 'OBRAS'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Localidad":"localidad",
            "CUIT":"cuit",
            "Imputacion":"actividad",
            "Partida":"partida",
            "Fuente":"fuente",
            "MontoDeContrato":"monto_contrato",
            "Adicional":"adicional",
            "Cuenta":"cta_cte",
            "NormaLegal":"norma_legal",
            "Descripcion":"obra",
            "InformacionAdicional":"info_adicional",
            }, inplace=True)
        self._TABLE_NAME = 'obras'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_carga(self) -> pd.DataFrame:
        """"Migrate table carga"""
        self._TABLE_NAME = 'CARGA'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Fecha":"fecha",
            "Fuente":"fuente",
            "CUIT":"cuit",
            "Importe":"importe",
            "FondoDeReparo":"fondo_reparo",
            "Cuenta":"cta_cte",
            "Avance":"avance",
            "Certificado":"certificado",
            "Comprobante":"nro_comprobante",
            "Obra":"obra",
            "Origen":"origen",
            "Tipo":"tipo",
            "Imputacion":"actividad",
            "Partida":"partida",
            }, inplace=True)
        self.df['fecha'] = pd.TimedeltaIndex(self.df['fecha'], unit='d') + dt.datetime(1970,1,1)
        self.df['id'] = self.df['nro_comprobante'] + 'C'
        self.df.loc[self.df['tipo'] == 'PA6', 'id'] = self.df['nro_comprobante'] + 'F'
        self._TABLE_NAME = 'carga'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_retenciones(self) -> pd.DataFrame:
        """"Migrate table retenciones"""
        self._TABLE_NAME = 'RETENCIONES'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "Codigo":"codigo",
            "Importe":"importe",
            "Comprobante":"nro_comprobante",
            "Tipo":"tipo",
            }, inplace=True)
        self.df['id_carga'] = self.df['nro_comprobante'] + 'C'
        self.df.loc[self.df['tipo'] == 'PA6', 'id_carga'] = self.df['nro_comprobante'] + 'F'
        self.df.drop(['nro_comprobante', 'tipo'], axis=1, inplace=True)
        self._TABLE_NAME = 'retenciones'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_certificados_obras(self) -> pd.DataFrame:
        """"Migrate table certificados_obras"""
        self._TABLE_NAME = 'CERTIFICADOS'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "NroComprobanteSIIF":"nro_comprobante",
            "TipoComprobanteSIIF":"tipo",
            "Origen":"origen",
            "Periodo":"ejercicio",
            "Beneficiario":"beneficiario",
            "Obra":"obra",
            "NroCertificado":"nro_certificado",
            "MontoCertificado":"monto_certificado",
            "FondoDeReparo":"fondo_reparo",            
            "ImporteBruto":"importe_bruto",
            "IIBB":"iibb",
            "LP":"lp",
            "SUSS":"suss",
            "GCIAS":"gcias",
            "INVICO":"invico",
            "ImporteNeto":"importe_neto",
            }, inplace=True)
        self.df['otros'] = 0
        self.df['cod_obra'] = self.df['obra'].str.split(' ', n=1).str[0]
        self.df.loc[self.df['nro_comprobante'] != '', 'id_carga'] = self.df['nro_comprobante'] + 'C'
        self.df.loc[self.df['tipo'] == 'PA6', 'id_carga'] = self.df['nro_comprobante'] + 'F'
        self.df.drop(['nro_comprobante', 'tipo'], axis=1, inplace=True)
        self._TABLE_NAME = 'certificados_obras'
        self.to_sql(self.path_new_icaro, True)

    # --------------------------------------------------
    def migrate_resumen_rend_obras(self) -> pd.DataFrame:
        """"Migrate table resumen_rend_obras"""
        self._TABLE_NAME = 'EPAM'
        self.df = self.from_sql(self.path_old_icaro)
        self.df.rename(columns={
            "NroComprobanteSIIF":"nro_comprobante",
            "TipoComprobanteSIIF":"tipo",
            "Origen":"origen",
            "Obra":"obra",
            "Periodo":"ejercicio",
            "Beneficiario":"beneficiario",
            "LibramientoSGF":"libramiento_sgf",
            "FechaPago":"fecha",            
            "ImporteBruto":"importe_bruto",
            "IIBB":"iibb",
            "TL":"lp",
            "Sellos":"sellos",
            "SUSS":"suss",
            "GCIAS":"gcias",
            "ImporteNeto":"importe_neto",
            }, inplace=True)
        self.df['destino'] = ''
        self.df['movimiento'] = ''
        self.df['seguro'] = 0
        self.df['salud'] = 0
        self.df['mutual'] = 0
        self.df['cod_obra'] = self.df['obra'].str.split('-', n=1).str[0]
        self.df['fecha'] = pd.TimedeltaIndex(self.df['fecha'], unit='d') + dt.datetime(1970,1,1)
        self.df['ejercicio'] = self.df['fecha'].dt.year.astype(str)
        self.df['mes'] = self.df['fecha'].dt.month.astype(str).str.zfill(2) + '/' + self.df['ejercicio']
        self.df.loc[self.df['nro_comprobante'] != '', 'id_carga'] = self.df['nro_comprobante'] + 'C'
        self.df.loc[self.df['tipo'] == 'PA6', 'id_carga'] = self.df['nro_comprobante'] + 'F'
        self.df.drop(['nro_comprobante', 'tipo'], axis=1, inplace=True)
        self._TABLE_NAME = 'resumen_rend_obras'
        self.to_sql(self.path_new_icaro, True)

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Migrate from old Icaro.sqlite to new DB",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-o', '--old_icaro', 
        metavar = "old_icaro",
        default='ICARO.sqlite',
        type=str,
        help = "Path to old ICARO.sqlite to be migrated")

    parser.add_argument(
        '-n', '--new_icaro', 
        metavar = "new_icaro",
        default='new_icaro.sqlite',
        type=str,
        help = "Path to new icaro.sqlite")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    migrate_icaro = MigrateIcaro(dir_path +"/"+ args.old_icaro, 
                                dir_path +"/"+ args.new_icaro)
    migrate_icaro.migrate_all()
    # df = migrate_icaro.from_sql(dir_path +"/"+ args.new_icaro, 'carga')
    # print(df.head(5))

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.icaro.migrate_icaro
