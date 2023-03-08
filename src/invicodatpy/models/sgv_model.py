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
class SGVModel():
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
        self.barrios_nuevos = Table(
            'barrios_nuevos', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('cod_barrio', String(5)),
            Column('barrio', String(150)),
            Column('localidad', String(50)),
            Column('q_entregadas', Numeric(5,0)),
            Column('importe_total', Numeric(12,2)),
            Column('importe_promedio', Numeric(12,2)),
        )

        self.saldo_barrio_variacion = Table(
            'saldo_barrio_variacion', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('cod_barrio', String(5)),
            Column('barrio', String(150)),
            Column('saldo_inicial', Numeric(12,2)),
            Column('amortizacion',Numeric(12,2)),
            Column('cambios', Numeric(12,2)),
            Column('saldo_final', Numeric(12,2)),
        )

        self.saldo_motivo = Table(
            'saldo_motivo', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('cod_motivo', String(5)),
            Column('motivo', String(150)),
            Column('importe', Numeric(12,2)),
        )

        self.saldo_motivo_individual = Table(
            'saldo_motivo_individual', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('cod_motivo', String(5)),
            Column('motivo', String(20)),
            Column('cod_barrio', String(5)),
            Column('barrio', String(150)),
            Column('importe', Numeric(12,2)),
        )

        self.resumen_facturado = Table(
            'resumen_facturado', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(4)),
            Column('amortizacion', Numeric(12,2)),
            Column('int_financiero', Numeric(12,2)),
            Column('int_mora', Numeric(12,2)),
            Column('gtos_adm', Numeric(12,2)),
            Column('seg_incendio', Numeric(12,2)),
            Column('seg_vida', Numeric(12,2)),
            Column('subsidio', Numeric(12,2)),
            Column('pago_amigable', Numeric(12,2)),
            Column('escritura', Numeric(12,2)),
            Column('facturado_total', Numeric(12,2)),
        )

        self.resumen_recaudado = Table(
            'resumen_recaudado', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(4)),
            Column('amortizacion', Numeric(12,2)),
            Column('int_financiero', Numeric(12,2)),
            Column('int_mora', Numeric(12,2)),
            Column('gtos_adm', Numeric(12,2)),
            Column('seg_incendio', Numeric(12,2)),
            Column('seg_vida', Numeric(12,2)),
            Column('subsidio', Numeric(12,2)),
            Column('pago_amigable', Numeric(12,2)),
            Column('escritura', Numeric(12,2)),
            Column('pend_acreditacion', Numeric(12,2)),
            Column('recaudado_total', Numeric(12,2)),
        )

        self.saldo_barrio = Table(
            'saldo_barrio', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('cod_barrio', String(5)),
            Column('barrio', String(150)),
            Column('saldo_vencido', Numeric(12,2)),
            Column('saldo_actual',Numeric(12,2)),
            Column('localidad', String(50)),
        )

        self.saldo_recuperos_cobrar_variacion = Table(
            'saldo_recuperos_cobrar_variacion', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('concepto', String(50)),
            Column('importe', Numeric(12,2)),
        )

    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)