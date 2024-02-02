#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Web Scrapping SIIF with selenium
Source: 
"""

import os
import time
from dataclasses import dataclass, field

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

@dataclass
class ConnectSIIF():
    username:str = None 
    password:str = None
    invisible:bool = False
    driver:webdriver = field(init=False, repr=False, default=None)

    # --------------------------------------------------
    def __post_init__(self):
        # Innitial driver options
        options = Options()
        options.add_argument("--window-size=1920,1080")
        if self.invisible:
            options.add_argument("--headless")
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        self.driver.maximize_window()

        # Setup wait for later
        self.wait = WebDriverWait(self.driver, 10)
        
        self.connect()
        self.go_to_reportes()

    # --------------------------------------------------
    def connect(self) -> None:
        """"Connect SIIF"""
        self.driver.get('https://siif.cgpc.gob.ar/mainSiif/faces/login.jspx')
        try:
            self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='pt1:t1::oc']")))
            input_username, input_password = self.driver.find_elements(By.XPATH, "//input[starts-with(@id,'pt')]")
            input_username.send_keys(self.username)
            input_password.send_keys(self.password) 
            btn_connect = self.driver.find_element(By.XPATH, "//div[@id='pt1:t1::oc']")
            btn_connect.click()
            time.sleep(1)
        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.quit()

    # --------------------------------------------------
    def go_to_reportes(self) -> None:
        """"Go to SIIF's Reportes Module"""
        try:
            #Abrir pestaña reportes
            self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//a[@class='xyj' and text()='REPORTES']"))
            )
            mnu_reportes = self.driver.find_element(By.XPATH, "//a[@class='xyj' and text()='REPORTES']")
            mnu_reportes.click()
            time.sleep(1)
            reportes = self.driver.find_element(By.XPATH, "//tr[@id='pt1:cmiReportes']")
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
    def disconnect(self) -> None:
        self.driver.switch_to.window(self.driver.window_handles[0])
        btn_disconnect = self.driver.find_element(By.XPATH, "//a[@id='pt1:tnp1:tcni1']")
        btn_disconnect.click()
        self.quit()

    # --------------------------------------------------
    def quit(self) -> None:
        # Quit
        self.driver.quit()