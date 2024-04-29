#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
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
        if not os.path.exists(self.sql_path):
            self.metadata = MetaData()
            self.model_tables()
            self.create_engine()
            self.create_database()
        else:
            self.create_engine()

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
            Column('es_comprometido', Boolean()),
            Column('es_verificado', Boolean()),
            Column('es_aprobado',Boolean()),
            Column('es_pagado', Boolean())
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
            Column('nro_original', String(6)),
            Column('auxiliar_1', String(50)),
            Column('auxiliar_2', String(50)),
            Column('tipo_comprobante', String(3)),
            Column('creditos', Numeric(12,2)),
            Column('debitos', Numeric(12,2)),
            Column('saldo', Numeric(12,2))
        )

        self.resumen_fdos_rfondo07tp = Table(
            'resumen_fdos_rfondo07tp', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('tipo_comprobante', String(50)),
            Column('nro_comprobante', String(8)),
            Column('nro_fondo', String(6)),
            Column('glosa', String(100)),
            Column('ingresos', Numeric(12,2)),
            Column('egresos', Numeric(12,2)),
            Column('saldo', Numeric(12,2))
        )

        self.comprobantes_rec_rci02 = Table(
            'comprobantes_rec_rci02', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('fuente', String(2)),
            Column('cta_cte', String(20)),
            Column('nro_entrada', String(6)),
            Column('importe', Numeric(12,2)),
            Column('glosa', String(100)),
            Column('es_remanente', Boolean()),
            Column('es_invico', Boolean()),
            Column('es_verificado', Boolean()),
            Column('clase_reg', String(3)),
            Column('clase_mod', String(3))
        )

        self.ppto_rec_ri102 = Table(
            'ppto_rec_ri102', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('tipo', String(5)),
            Column('clase', String(5)),
            Column('cod_rec', String(5)),
            Column('desc_rec', String(50)),
            Column('fuente', String(2)),
            Column('org_fin', String(3)),
            Column('ppto_inicial', Numeric(12,2)),
            Column('ppto_modif', Numeric(12,2)),
            Column('ppto_vigente', Numeric(12,2)),
            Column('ingreso', Numeric(12,2)),
            Column('saldo', Numeric(12,2)),
        )

        self.deuda_flotante_rdeu012 = Table(
            'deuda_flotante_rdeu012', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('mes_hasta', String(7)),
            Column('fuente', String(2)),
            Column('cta_cte', String(20)),
            Column('nro_comprobante', String(8)),
            Column('importe', Numeric(12,2)),
            Column('saldo', Numeric(12,2)),
            Column('cuit', String(11)),
            Column('beneficiario', String(50)),
            Column('glosa', String(100)),
            Column('nro_expte', String(12)),
            Column('nro_entrada', String(6)),
            Column('nro_origen', String(6)),
            Column('fecha_aprobado', Date()),
            Column('fecha_desde', Date()),
            Column('fecha_hasta', Date()),
            Column('org_fin', String(1))
        )

        self.deuda_flotante_rdeu012b2_c = Table(
            'deuda_flotante_rdeu012b2_c', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes_hasta', String(7)),
            Column('entidad', String(50)),
            Column('ejercicio_deuda', String(4)),
            Column('fuente', String(2)),
            Column('nro_entrada', String(6)),
            Column('nro_origen', String(6)),
            Column('importe', Numeric(12,2)),
            Column('saldo', Numeric(12,2)),
            Column('org_fin', String(3)),
            Column('nro_expte', String(12)),
            Column('cta_cte', String(20)),
            Column('glosa', String(100)),
            Column('fecha_desde', Date()),
            Column('fecha_hasta', Date()),
        )

        self.detalle_partidas = Table(
            'detalle_partidas', self.metadata,
            Column('partida', String(3), primary_key=True),
            Column('desc_partida', String(75)),
            Column('part_parcial', String(3)),
            Column('desc_part_parcial', String(50)),
            Column('grupo', String(3)),
            Column('desc_grupo', String(50)),
        )

        self.form_gto_rfp_p605b = Table(
            'form_gto_rfp_p605b', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('estructura', String(15)),
            Column('fuente', String(2)),
            Column('programa', String(2)),
            Column('desc_prog', String(50)),
            Column('subprograma', String(2)),
            Column('desc_subprog', String(50)),
            Column('proyecto', String(2)),
            Column('desc_proy', String(50)),
            Column('actividad', String(2)),
            Column('desc_act', String(50)),
            Column('grupo', String(3)),
            Column('partida', String(3)),
            Column('formulado', Numeric(12,2)),
        )

    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)