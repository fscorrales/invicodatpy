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

@pytest.fixture()
def setup_and_teardown_siif(request):
    hp = HanglingPath()
    json_path = os.path.join(hp.get_siif_path(), 'siif_credentials.json')
    with open(json_path) as json_file:
        data_json = json.load(json_file)
        request.cls.username = data_json['username']
        request.cls.password = data_json['password']
    json_file.close()
    connect_siif = ConnectSIIF()
    request.cls.connect_siif = connect_siif
    yield
    connect_siif.quit()