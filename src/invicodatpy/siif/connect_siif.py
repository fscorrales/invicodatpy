#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Web Scrapping SIIF with selenium
Source: 
"""

import os
import time
from dataclasses import dataclass, field

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

@dataclass
class ConnectSIIF():
    username:str = None 
    password:str = None
    driver:webdriver = field(init=False, repr=False, default=None)

    # --------------------------------------------------
    def __post_init__(self):
        self.connect()
        self.go_to_reportes()

    # --------------------------------------------------
    def connect(self) -> None:
        """"Connect SIIF"""
        options = Options()
        options.add_argument("--window-size=1920,1080")

        self.driver = webdriver.Chrome(options=options)
        self.driver.maximize_window()

        self.driver.get('https://siif.cgpc.gob.ar/mainSiif/faces/login.jspx')
        input_username, input_password = self.driver.find_elements(By.XPATH, "//input[starts-with(@id,'pt')]")
        input_username.send_keys(self.username) #'27corralesfs'
        input_password.send_keys(self.password) #'fsc188'
        btn_connect = self.driver.find_element(By.XPATH, "//div[@id='pt1:t1::oc']")
        btn_connect.click()
        time.sleep(1)


    # --------------------------------------------------
    def go_to_reportes(self) -> None:
        """"Go to SIIF's Reportes Module"""      
        #Abrir pestaña reportes
        mnu_reportes = self.driver.find_element(By.XPATH, "//a[@class='xyj' and text()='REPORTES']")
        mnu_reportes.click()
        time.sleep(1)
        reportes = self.driver.find_element(By.XPATH, "//tr[@id='pt1:cmiReportes']")
        reportes.click()
        time.sleep(1)
        self.driver.switch_to.window(self.driver.window_handles[1])
        time.sleep(1)

    # --------------------------------------------------
    def rename_report(self, dir_path:str, old_name:str, new_name:str):
        old_file_path = os.path.join(dir_path, old_name)
        new_file_path = os.path.join(dir_path, new_name)
        while not os.path.exists(old_file_path):
            time.sleep(1)

        if os.path.isfile(old_file_path):
            if os.path.isfile(new_file_path):
                os.remove(new_file_path)
            os.rename(old_file_path, new_file_path)
        else:
            raise ValueError("%s isn't a file!" % old_file_path)

    # --------------------------------------------------
    def disconnect(self) -> None:
        self.driver.switch_to.window(self.driver.window_handles[0])
        btn_disconnect = self.driver.find_element(By.XPATH, "//a[@id='pt1:tnp1:tcni1']")
        btn_disconnect.click()