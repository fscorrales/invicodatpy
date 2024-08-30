#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Automate SSCC with pywinauto
Source: 
"""

__all__ = ['ConnectSSCC']

import os
import time
from dataclasses import dataclass, field

from pywinauto import findwindows, keyboard, mouse
from pywinauto.application import Application


@dataclass
class ConnectSSCC():
    username:str = None 
    password:str = None

    # --------------------------------------------------
    def __post_init__(self):        
        self.connect()
        # self.go_to_reports()

    # --------------------------------------------------
    def connect(self) -> None:
        """"Connect SIIF"""
        app_path = r"\\ipvfiles\SISTEMAS\Bancos\Bancos.exe"
        self.app = Application(backend='uia').start(app_path)
        try:
            self.main = self.app.window(title_re=".*Sistema de Seguimiento de Cuentas Corrientes.*")
            if self.main.is_maximized() == False:
                self.main.maximize()
            cmb_user = self.main.child_window(auto_id="1", control_type="ComboBox").wait('exists enabled visible ready')
            input_password = self.main.child_window(auto_id="2", control_type="Edit").wrapper_object()
            cmb_user.type_keys(self.username)
            input_password.type_keys(self.password)
            btn_accept = self.main.child_window(title="Aceptar", auto_id="4", control_type="Button").wrapper_object()
            btn_accept.click()
            window_resumen_ctas = self.main.child_window(
                title="Resumen General de Cuentas", auto_id="32768", control_type="Window"
            ).wait('exists enabled visible ready')
            mouse.click(coords=(800, 60))
            time.sleep(2)
        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.quit()

    # # --------------------------------------------------
    # def go_to_reports(self) -> None:
    #     """"Go to SIIF's Reportes Module"""
    #     try:
    #         #Abrir pestaña reportes
    #         self.wait.until(EC.presence_of_element_located(
    #             (By.XPATH, "//a[@class='xyj' and text()='REPORTES']"))
    #         )
    #         mnu_reportes = self.driver.find_element(By.XPATH, "//a[@class='xyj' and text()='REPORTES']")
    #         mnu_reportes.click()
    #         time.sleep(1)
    #         reportes = self.driver.find_element(By.XPATH, "//tr[@id='pt1:cmiReportes']")
    #         reportes.click()
    #         # Wait for the new window or tab
    #         self.wait.until(EC.number_of_windows_to_be(2))
    #         self.driver.switch_to.window(self.driver.window_handles[1])
    #         self.wait.until(EC.presence_of_element_located(
    #             (By.XPATH, "//select[@id='pt1:socModulo::content']"))
    #         )
    #         # time.sleep(1)
    #     except Exception as e:
    #         print(f"Ocurrió un error: {e}, {type(e)}")
    #         self.disconnect() 

    # --------------------------------------------------
    def move_report(self, dir_path:str, name:str):
        old_file_path = os.path.join(r"D:\Users\fcorrales\Desktop", name)
        new_file_path = os.path.join(dir_path, name)
        while not os.path.exists(old_file_path):
            time.sleep(1)

        if os.path.isfile(old_file_path):
            if os.path.isfile(new_file_path):
                os.remove(new_file_path)
            os.rename(old_file_path, new_file_path)
        else:
            raise ValueError("%s isn't a file!" % old_file_path)

    # --------------------------------------------------
    def quit(self) -> None:
        # Quit
        keyboard.send_keys('%s')
