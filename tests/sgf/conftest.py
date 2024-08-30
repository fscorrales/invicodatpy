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
from src.invicodatpy.sgf import ConnectSGF, ResumenRendProv

# hp = HanglingPath()
ejercicio = '2024'

def get_sgf_username_and_password() -> tuple:
    json_path = os.path.join(HanglingPath.get_sgf_path(), 'sgf_credentials.json')
    with open(json_path) as json_file:
        data_json = json.load(json_file)
        username = data_json['username']
        password = data_json['password']
    json_file.close()
    return username, password

@pytest.fixture()
def setup_and_teardown_sgf(request):
    request.cls.username, request.cls.password = get_sgf_username_and_password()
    connect_sgf = ConnectSGF()
    request.cls.connect_sgf = connect_sgf
    yield
    connect_sgf.quit()

@pytest.fixture(scope = 'class')
def setup_and_teardown_sgf_rend_prov(request):
    request.cls.ejercicio= ejercicio
    request.cls.origenes= ['EPAM'] #['EPAM', 'OBRAS', 'FUNCIONAMIENTO']
    test_sgf_path = HanglingPath.get_test_sgf_path()
    request.cls.test_sgf_path = test_sgf_path
    username, password = get_sgf_username_and_password()
    connect_sgf = ConnectSGF(username = username, password = password)
    request.cls.connect_sgf = connect_sgf
    request.cls.sgf_rend_prov = ResumenRendProv(sgf = connect_sgf)
    yield
    connect_sgf.quit()