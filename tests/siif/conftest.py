import pytest
import json

import sys
import os

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))
 
# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)
parent = os.path.dirname(parent)

# adding the parent directory to 
# the sys.path.
sys.path.append(parent)

# importing
from tests.utils.hangling_path import HanglingPath
from src.invicodatpy.siif.connect_siif import ConnectSIIF
from src.invicodatpy.siif.resumen_contable_cta_rvicon03 import ResumenContableCtaRvicon03
from src.invicodatpy.siif.ppto_gtos_fte_rf602 import PptoGtosFteRf602
from src.invicodatpy.siif.comprobantes_gtos_rcg01_uejp import ComprobantesGtosRcg01Uejp
from src.invicodatpy.siif.comprobantes_gtos_gpo_part_gto_rpa03g import ComprobantesGtosGpoPartGtoRpa03g

hp = HanglingPath()
ejercicio = '2022'

def get_siif_username_and_password() -> tuple:
    json_path = os.path.join(hp.get_siif_path(), 'siif_credentials.json')
    with open(json_path) as json_file:
        data_json = json.load(json_file)
        username = data_json['username']
        password = data_json['password']
    json_file.close()
    return username, password

@pytest.fixture()
def setup_and_teardown_siif(request):
    request.cls.username, request.cls.password = get_siif_username_and_password()
    connect_siif = ConnectSIIF()
    request.cls.connect_siif = connect_siif
    yield
    connect_siif.quit()

@pytest.fixture(scope = 'class')
def setup_and_teardown_siif_rvicon03(request):
    request.cls.ejercicio= ejercicio
    test_siif_path = hp.get_test_siif_path()
    request.cls.test_siif_path = test_siif_path
    username, password = get_siif_username_and_password()
    connect_siif = ConnectSIIF(username = username, password = password)
    request.cls.connect_siif = connect_siif
    request.cls.resumen_contable_cta_rvicon03 = ResumenContableCtaRvicon03(siif = connect_siif)
    yield
    connect_siif.quit()

@pytest.fixture(scope = 'class')
def setup_and_teardown_siif_rf602(request):
    request.cls.ejercicio= ejercicio
    test_siif_path = hp.get_test_siif_path()
    request.cls.test_siif_path = test_siif_path
    username, password = get_siif_username_and_password()
    connect_siif = ConnectSIIF(username = username, password = password)
    request.cls.connect_siif = connect_siif
    request.cls.rf602 = PptoGtosFteRf602(siif = connect_siif)
    yield
    connect_siif.quit()

@pytest.fixture(scope = 'class')
def setup_and_teardown_siif_rcg01_uejp(request):
    request.cls.ejercicio= ejercicio
    test_siif_path = hp.get_test_siif_path()
    request.cls.test_siif_path = test_siif_path
    username, password = get_siif_username_and_password()
    connect_siif = ConnectSIIF(username = username, password = password)
    request.cls.connect_siif = connect_siif
    request.cls.rcg01_uejp = ComprobantesGtosRcg01Uejp(siif = connect_siif)
    yield
    connect_siif.quit()

@pytest.fixture(scope = 'class')
def setup_and_teardown_siif_rpa03g(request):
    request.cls.ejercicio= ejercicio
    test_siif_path = hp.get_test_siif_path()
    request.cls.test_siif_path = test_siif_path
    username, password = get_siif_username_and_password()
    connect_siif = ConnectSIIF(username = username, password = password)
    request.cls.connect_siif = connect_siif
    request.cls.rpa03g = ComprobantesGtosGpoPartGtoRpa03g(siif = connect_siif)
    yield
    connect_siif.quit()