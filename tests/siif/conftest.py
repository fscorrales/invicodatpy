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
from src.invicodatpy.siif import (
    ConnectSIIF, PptoGtosFteRf602, ComprobantesGtosRcg01Uejp, 
    ComprobantesGtosGpoPartGtoRpa03g, ResumenContableCtaRvicon03,
    ResumenFdosRfondo07tp, PptoGtosDescRf610, ComprobantesRecRci02,
    DeudaFlotanteRdeu012
)

hp = HanglingPath()
ejercicio = '2024'

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
def setup_and_teardown_siif_rf610(request):
    request.cls.ejercicio = ejercicio
    test_siif_path = hp.get_test_siif_path()
    request.cls.test_siif_path = test_siif_path
    username, password = get_siif_username_and_password()
    ConnectSIIF.connect(username = username, password = password)
    request.cls.rf610 = PptoGtosDescRf610()
    yield
    request.cls.rf610.quit()

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
def setup_and_teardown_siif_gto_rpa03g(request):
    request.cls.ejercicio= ejercicio
    test_siif_path = hp.get_test_siif_path()
    request.cls.test_siif_path = test_siif_path
    username, password = get_siif_username_and_password()
    connect_siif = ConnectSIIF(username = username, password = password)
    request.cls.connect_siif = connect_siif
    request.cls.gto_rpa03g = ComprobantesGtosGpoPartGtoRpa03g(siif = connect_siif)
    yield
    connect_siif.quit()

@pytest.fixture(scope = 'class')
def setup_and_teardown_siif_rfondo07tp(request):
    request.cls.ejercicio= ejercicio
    test_siif_path = hp.get_test_siif_path()
    request.cls.test_siif_path = test_siif_path
    username, password = get_siif_username_and_password()
    connect_siif = ConnectSIIF(username = username, password = password)
    request.cls.connect_siif = connect_siif
    request.cls.rfondo07tp = ResumenFdosRfondo07tp(siif = connect_siif)
    yield
    connect_siif.quit()


@pytest.fixture(scope = 'class')
def setup_and_teardown_siif_rci02(request):
    request.cls.ejercicio= ejercicio
    test_siif_path = hp.get_test_siif_path()
    request.cls.test_siif_path = test_siif_path
    username, password = get_siif_username_and_password()
    connect_siif = ConnectSIIF(username = username, password = password)
    request.cls.connect_siif = connect_siif
    request.cls.rci02 = ComprobantesRecRci02(siif = connect_siif)
    yield
    connect_siif.quit()

@pytest.fixture(scope = 'class')
def setup_and_teardown_siif_rdeu012(request):
    request.cls.ejercicio_mes = ejercicio + '01'
    test_siif_path = hp.get_test_siif_path()
    request.cls.test_siif_path = test_siif_path
    username, password = get_siif_username_and_password()
    connect_siif = ConnectSIIF(username = username, password = password)
    request.cls.connect_siif = connect_siif
    request.cls.rdeu012 = DeudaFlotanteRdeu012(siif = connect_siif)
    yield
    connect_siif.quit()