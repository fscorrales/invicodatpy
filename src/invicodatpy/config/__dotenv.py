__all__ = ['SIIF_USERNAME', 'SIIF_PASSWORD']


import os

from dotenv import load_dotenv

# Cargamos nuestras variables de entorno
load_dotenv()


SIIF_USERNAME = os.getenv("siif_username")
if not SIIF_USERNAME:
    raise Exception("SIIF's login username string not found")

SIIF_PASSWORD = os.getenv("siif_password")
if not SIIF_PASSWORD:
    raise Exception("SIIF's login password string not found")