#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: SIIF database model
"""

import os
from dataclasses import dataclass

from sqlalchemy import (Boolean, Column, Date, Integer, MetaData, Numeric,
                        String, Table, create_engine)


@dataclass
class SIIFModel():
    sql_path:str

    def __post_init__(self):
        self.metadata = MetaData()
        self.model_tables()
        self.create_engine()
        self.create_database()

    def model_tables(self):
        """Create table models"""
        self.ppto_gtos_fte_rf602 = Table(
            'ppto_gtos_fte_rf602', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('estructura', String(15)),
            Column('fuente', String(2)),
            Column('programa', String(2)),
            Column('subprograma', String(2)),
            Column('proyecto', String(2)),
            Column('actividad', String(2)),
            Column('grupo', String(3)),
            Column('partida', String(3)),
            Column('org', String(1)),
            Column('credito_original', Numeric(12,2)),
            Column('credito_vigente', Numeric(12,2)),
            Column('comprometido', Numeric(12,2)),
            Column('ordenado', Numeric(12,2)),
            Column('saldo', Numeric(12,2)),
            Column('pendiente', Numeric(12,2))
        )

        self.ppto_gtos_desc_rf610 = Table(
            'ppto_gtos_desc_rf610', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('estructura', String(15)),
            Column('programa', String(2)),
            Column('desc_prog', String(50)),
            Column('subprograma', String(2)),
            Column('desc_subprog', String(50)),
            Column('proyecto', String(2)),
            Column('desc_proy', String(50)),
            Column('actividad', String(2)),
            Column('desc_act', String(50)),
            Column('grupo', String(3)),
            Column('desc_gpo', String(50)),
            Column('partida', String(3)),
            Column('desc_part', String(50)),
            Column('credito_original', Numeric(12,2)),
            Column('credito_vigente', Numeric(12,2)),
            Column('comprometido', Numeric(12,2)),
            Column('ordenado', Numeric(12,2)),
            Column('saldo', Numeric(12,2))
        )

        self.comprobantes_gtos_rcg01_uejp = Table(
            'comprobantes_gtos_rcg01_uejp', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('nro_comprobante', String(8)),
            Column('importe', Numeric(12,2)),
            Column('fuente', String(2)),
            Column('cta_cte', String(20)),
            Column('cuit', String(11)),
            Column('nro_expte', String(12)),
            Column('nro_fondo', String(5)),
            Column('nro_entrada', String(5)),
            Column('nro_origen', String(5)),
            Column('clase_reg', String(3)),
            Column('clase_mod', String(3)),
            Column('clase_gto', String(3)),
            Column('beneficiario', String(50)),
            Column('comprometido', Boolean()),
            Column('verificado', Boolean()),
            Column('aprobado',Boolean()),
            Column('pagado', Boolean())
        )

        self.comprobantes_gtos_gpo_part_gto_rpa03g = Table(
            'comprobantes_gtos_gpo_part_gto_rpa03g', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('nro_comprobante', String(8)),
            Column('importe', Numeric(12,2)),
            Column('grupo', String(3)),
            Column('partida', String(3)),
            Column('nro_entrada', String(5)),
            Column('nro_origen', String(5)),
            Column('nro_expte', String(12)),
            Column('glosa', String(100)),
            Column('beneficiario', String(50))
        )

        self.mayor_contable_rcocc31 = Table(
            'mayor_contable_rcocc31', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('fecha_aprobado', Date()),
            Column('cta_contable', String(8)),
            Column('nro_entrada', String(6)),
            Column('auxiliar_1', String(50)),
            Column('auxiliar_2', String(50)),
            Column('tipo_comprobante', String(3)),
            Column('creditos', Numeric(12,2)),
            Column('debitos', Numeric(12,2)),
            Column('saldo', Numeric(12,2))
        )

    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)