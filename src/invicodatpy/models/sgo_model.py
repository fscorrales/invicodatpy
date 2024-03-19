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
class SGOModel():
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
        self.listado_obras = Table(
            'listado_obras', self.metadata,
            Column('id', Integer(), autoincrement=True, primary_key=True),
            Column('cod_obra', String(9)),
            Column('mes_basico_obra', String(7)),
            Column('cod_contrato', String(9)),
            Column('obra', String(200)),
            Column('mes_basico_contrato', String(7)),
            Column('tipo_obra', String(50)),
            Column('localidad', String(50)),
            Column('contratista', String(100)),
            Column('monto', Numeric(12,2)),
            Column('monto_total', Numeric(12,2)),
            Column('representante', String(100)),
            Column('operatoria', String(100)),
            Column('q_rubros', String(2)),
            Column('id_inspector', String(2)),
            Column('iniciador', String(100)),
            Column('estado', String(50)),
            Column('fecha_inicio', Date()),
            Column('fecha_contrato', Date()),
            Column('fecha_fin', Date()),
            Column('plazo_est_dias', String(3)),
            Column('fecha_fin_est', Date()),
            Column('plazo_ampl_est_dias', String(3)),
            Column('fecha_fin_ampl_est', Date()),
            Column('avance_fis_real', Numeric(1,4)),
            Column('avance_fciero_real', Numeric(1,4)),
            Column('avance_fis_est', Numeric(1,4)),
            Column('avance_fciero_est', Numeric(1,4)),
            Column('monto_certificado', Numeric(12,2)),
            Column('monto_certificado_obra', Numeric(12,2)),
            Column('monto_pagado', Numeric(5,2)),
            Column('nro_ultimo_certif', String(2)),
            Column('nro_ultimo_certif_bc', String(2)),
            Column('mes_obra_certif', String(7)),
            Column('fecha_ultimo_certif', Date()),
            Column('anticipo_acum', Numeric(12,2)),
            Column('cant_anticipo', String(2)),
            Column('porc_anticipo', Numeric(1,4)),
            Column('cant_certif_anticipo', String(2)),
            Column('fdo_reparo_acum', Numeric(12,2)),
            Column('desc_fdo_reparo_acum', Numeric(12,2)),
            Column('monto_redeterminado', Numeric(12,2)),
            Column('nro_ultima_rederterminacion', String(2)),
            Column('mes_ultimo_basico', String(7)),
            Column('nro_ultima_medicion', String(2)),
            Column('mes_ultima_medicion', String(7)),
        )


    def create_engine(self):
        """Create an SQLite DB engine"""
        self.engine = create_engine(f'sqlite:///{self.sql_path}')

    def create_database(self):
        """Create DataBase from engine"""
        self.metadata.create_all(self.engine)