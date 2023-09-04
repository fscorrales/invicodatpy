#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: SGF database model
"""

import os
from dataclasses import dataclass

from sqlalchemy import (Column, Date, Integer, MetaData, Numeric,
                        String, Table, create_engine)


@dataclass
class SGFModel():
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
        
        self.listado_prov = Table(
            'listado_prov', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('codigo', String(5)),
            Column('cuit', String(15)),
            Column('desc_prov', String(50)),
            Column('domicilio', String(50)),
            Column('localidad', String(30)),
            Column('telefono', String(30)),
            Column('condicion_iva', String(30))
        )

        self.certificados_obras = Table(
            'certificados_obras', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('beneficiario', String(50)),
            Column('cod_obra', String(10)),
            Column('obra', String(100)),
            Column('nro_certificado', String(6)),
            Column('monto_certificado', Numeric(12,2)),
            Column('fondo_reparo', Numeric(12,2)),
            Column('otros', Numeric(12,2)),
            Column('importe_bruto', Numeric(12,2)),
            Column('iibb', Numeric(12,2)),
            Column('lp', Numeric(12,2)),
            Column('suss', Numeric(12,2)),
            Column('gcias', Numeric(12,2)),
            Column('invico', Numeric(12,2)),
            Column('retenciones', Numeric(12,2)),
            Column('importe_neto', Numeric(12,2))
        )

        self.resumen_rend_obras = Table(
            'resumen_rend_obras', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('beneficiario', String(50)),
            Column('cod_obra', String(15)),
            Column('obra', String(100)),
            Column('destino', String(30)),
            Column('libramiento_sgf', String(10)),
            Column('movimiento', String(10)),
            Column('importe_bruto', Numeric(12,2)),
            Column('gcias', Numeric(12,2)),
            Column('sellos', Numeric(12,2)),
            Column('lp', Numeric(12,2)),
            Column('iibb', Numeric(12,2)),
            Column('suss', Numeric(12,2)),
            Column('seguro', Numeric(12,2)),
            Column('salud', Numeric(12,2)),
            Column('mutual', Numeric(12,2)),
            Column('retenciones', Numeric(12,2)),
            Column('importe_neto', Numeric(12,2))
        )

        self.resumen_rend_prov = Table(
            'resumen_rend_prov', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('origen', String(12)),
            Column('ejercicio', String(4)),
            Column('mes', String(7)),
            Column('fecha', Date()),
            Column('beneficiario', String(50)),
            Column('destino', String(30)),
            Column('libramiento_sgf', String(10)),
            Column('movimiento', String(10)),
            Column('cta_cte', String(20)),
            Column('importe_bruto', Numeric(12,2)),
            Column('gcias', Numeric(12,2)),
            Column('sellos', Numeric(12,2)),
            Column('iibb', Numeric(12,2)),
            Column('suss', Numeric(12,2)),
            Column('invico', Numeric(12,2)),
            Column('seguro', Numeric(12,2)),
            Column('salud', Numeric(12,2)),
            Column('mutual', Numeric(12,2)),
            Column('otras', Numeric(12,2)),
            Column('retenciones', Numeric(12,2)),
            Column('importe_neto', Numeric(12,2))
        )

    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)