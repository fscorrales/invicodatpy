#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Web Scrapping SIIF with selenium
Source: 
"""

__all__ = ['ConnectSIIF', 'ReportCategory']

import os
import time
from enum import Enum
from typing import Literal

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from ..utils import PrintTidyverse, SQLUtils, read_xls


class ReportCategory(Enum):
    Gastos = "SUB - SISTEMA DE CONTROL DE GASTOS"
    Recursos = "SUB - SISTEMA DE CONTROL de RECURSOS"
    Contabilidad = "SUB - SISTEMA DE CONTABILIDAD PATRIMONIAL"
    Formulacion = "SUB - SISTEMA DE FORMULACION PRESUPUESTARIA"
    Clasificadores = "SUB - SISTEMA DE CLASIFICADORES"


class ConnectSIIF(SQLUtils):
    username:str =  ''
    password:str = ''
    invisible:bool = False
    driver:webdriver = None
    wait:WebDriverWait

    # --------------------------------------------------
    def __init__(self, username:str = '', password:str = '', invisible:bool = False) -> None:
        if username != '' and password != '':
            self.connect(username, password, invisible)

    # --------------------------------------------------
    @classmethod
    def init_driver(cls):
        # Innitial driver options
        options = Options()
        options.add_argument("--window-size=1920,1080")
        if cls.invisible:
            options.add_argument("--headless")
        service = Service(ChromeDriverManager().install())
        cls.driver = webdriver.Chrome(service=service, options=options)
        cls.driver.maximize_window()

        # Setup wait for later
        cls.wait = WebDriverWait(cls.driver, 10)
        
        "Open SIIF webpage"
        cls.driver.get('https://siif.cgpc.gob.ar/mainSiif/faces/login.jspx')
        # self.connect()
        # self.go_to_reports()

    # --------------------------------------------------
    @classmethod
    def connect(cls, username:str = '', password:str = '', invisible:bool = False) -> None:
        if username != '':
            cls.username = username
        if password !='':
            cls.password = password
        cls.invisible = invisible
        cls.init_driver()

        #Probamos si hay connección al servidor
        server_down = cls.driver.find_elements(
            By.XPATH, "/html/body/div[3]/h3[contains(., 'SIIF no disponible!')]"
        )
        if len(server_down) > 0:
            raise WebDriverException("Servidor no disponible")

        try:
            cls.wait.until(EC.presence_of_element_located((By.XPATH, "//button[@id='pt1:cb1']")))
        except TimeoutException:
            print("No se pudo conectar con la página del SIIF. Verifique su conexión")
            cls.quit()

        try:
            input_username, input_password = cls.driver.find_elements(By.XPATH, "//input[starts-with(@id,'pt')]")
            input_username.send_keys(cls.username)
            input_password.send_keys(cls.password) 
            # input_password.send_keys(Keys.ENTER)  # Presionar la tecla Enter
            # time.sleep(1)
            # input_password.send_keys(Keys.ENTER)  # Presionar la tecla Enter
            btn_connect = cls.driver.find_element(By.XPATH, "//button[@id='pt1:cb1']")
            btn_connect.click()

            time.sleep(1)
        except NoSuchElementException:
            print(f"No se encontro el elemento: {NoSuchElementException}")
            cls.quit()

    # --------------------------------------------------
    def go_to_reports(self) -> None:
        """"Go to SIIF's Reportes Module"""
        try:
            #Abrir pestaña reportes
            mnu_reportes = self.get_dom_element("//button[@id='pt1:cb12']", wait=True)
            mnu_reportes.click()
            time.sleep(1)
            reportes = self.get_dom_element("//button[@id='pt1:cb14']", wait=True)
            reportes.click()
            # Wait for the new window or tab
            self.wait.until(EC.number_of_windows_to_be(2))
            self.driver.switch_to.window(self.driver.window_handles[1])
            self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//select[@id='pt1:socModulo::content']"))
            )
            # time.sleep(1)
        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.disconnect() 

    # --------------------------------------------------
    def select_report_module(self, module:ReportCategory) -> None:

        cmb_modulos = Select(
            self.get_dom_element("//select[@id='pt1:socModulo::content']", wait=True)
        )
        cmb_modulos.select_by_visible_text(module.value)
        time.sleep(1)

    # --------------------------------------------------
    def select_specific_report_by_id(self, report_id:str) -> None:
        input_filter = self.get_dom_element(
            "//input[@id='_afrFilterpt1_afr_pc1_afr_tableReportes_afr_c1::content']"
        )
        input_filter.clear()
        input_filter.send_keys(report_id, Keys.ENTER)
        btn_siguiente = self.get_dom_element("//div[@id='pt1:pc1:btnSiguiente']")
        btn_siguiente.click()

    # --------------------------------------------------
    def get_dom_element(self, value:str, by:Literal['id', 'xpath'] = 'xpath', wait:bool = False) -> WebElement:

        op_map = {
            "xpath": By.XPATH,
            "id": By.ID,
        }

        try:
            by_method = op_map[by]
        except KeyError:
            raise ValueError(f"Unknown module: {by}")

        if wait:
            self.wait.until(EC.presence_of_element_located(
                (by_method, value)
            ))
        return self.driver.find_element(by_method, value)

    # --------------------------------------------------
    def set_download_path(self, dir_path:str):
        # Path de salida
        params = {
        'behavior': 'allow',
        'downloadPath': dir_path
        }
        self.driver.execute_cdp_cmd('Page.setDownloadBehavior', params)

    # --------------------------------------------------
    def download_file_procedure(self) -> None:
        self.wait.until(EC.number_of_windows_to_be(3))
        self.driver.switch_to.window(self.driver.window_handles[2])
        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[1])

    # --------------------------------------------------
    def go_back_to_reports_list(self) -> None:
        btn_volver = self.get_dom_element("//div[@id='pt1:btnVolver']")
        btn_volver.click()
        time.sleep(1)

    # --------------------------------------------------
    def rename_report(self, dir_path:str, old_name:str, new_name:str):
        old_file_path = os.path.join(dir_path, old_name)
        new_file_path = os.path.join(dir_path, new_name)
        while not os.path.exists(old_file_path):
            time.sleep(1)

        # Si sigue sin funcionar bien, considerar la posibilidad 
        # de agregar un wait_is_there_html_file()

        if os.path.isfile(old_file_path):
            time.sleep(2)
            if os.path.isfile(new_file_path):
                os.remove(new_file_path)
                time.sleep(2)
            os.rename(old_file_path, new_file_path)
        else:
            raise ValueError("%s isn't a file!" % old_file_path)

    # --------------------------------------------------
    def remove_html_files(self, dir_path:str):
        """Remove html files"""
        time.sleep(5)
        file_list = os.listdir(dir_path)
        for f in file_list:
            if '.htm' in f:
                file_path = os.path.join(dir_path, f)
                os.remove(file_path)
                print(f"File: {file_path} removed")

    # --------------------------------------------------
    def read_xls(self, PATH:str, header:int = None) -> pd.DataFrame:
        return read_xls(PATH=PATH, header=header)

    # --------------------------------------------------
    def print_tidyverse(self, data = None):
        if data is None:
            data = self.df
        print(PrintTidyverse(data))

    # --------------------------------------------------
    def disconnect(self) -> None:
        self.driver.switch_to.window(self.driver.window_handles[0])
        btn_disconnect = self.get_dom_element("//a[@id='pt1:pt_np1:pt_cni1']")
        btn_disconnect.click()
        # self.quit()

    # --------------------------------------------------
    def quit(self) -> None:
        # Quit
        self.driver.quit()