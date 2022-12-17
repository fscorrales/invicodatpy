from setuptools import setup, find_packages

setup(
    name='invicodatpy',
    description="""
    Set of functions to read (input),
    modify (process) and write (output) different
    types of INVICO's (Instituto de Vivienda de Corrientes)
    files (xls, csv, sqlite). It's the first step of a
    posterior PyQt ecosystem. The database it's not
    provide within this package due to privacy reasons.
    """,
    author='Fernando Corrales',
    author_email='corrales_fernando@hotmail.com',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'datar',
        'datar-numpy',
        'datar-pandas',
        'df2gspread',
        'google-api-python-client',
        'google-auth',
        'google-auth-oauthlib',
        'SQLAlchemy',
        'pandas',
        'gspread',
        'xlrd',
        'openpyxl',
    ]
)
