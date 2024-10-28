#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Web Scrapping SIIF with playwright
Source: 
"""

__all__ = ['ConnectSIIF']

import os
import time
from dataclasses import dataclass, field

from playwright.async_api import async_playwright
from playwright.browser import Browser



@dataclass
class ConnectSIIF():
    username:str =  ''
    password:str = ''
    headless:bool = False
    driver:Browser = field(init=False, repr=False, default=None)

    # --------------------------------------------------
    async def __post_init__(self):
        self.playwright = await async_playwright().start()
        self.driver = await self.playwright.chromium.launch(
            headless=self.headless,
            args=["--start-maximized"]
        )
        self.context = await self.driver.new_context(no_viewport=True)
        self.page = await self.context.new_page()
        
        "Open SIIF webpage"
        self.page('https://siif.cgpc.gob.ar/mainSiif/faces/login.jspx')
        # self.connect()
        # self.go_to_reports()

    # --------------------------------------------------
    async def connect(self, username:str = '', password:str = '') -> None:
        if username != '':
            self.username = username
        if password !='':
            self.password = password

        #Probamos si hay connección al servidor
        # server_down = self.driver.find_elements(
        #     By.XPATH, "/html/body/div[3]/h3[contains(., 'SIIF no disponible!')]"
        # )
        # if len(server_down) > 0:
        #     raise WebDriverException("Servidor no disponible")

        # try:
        #     self.wait.until(EC.presence_of_element_located((By.XPATH, "//button[@id='pt1:cb1']")))
        # except TimeoutException:
        #     print("No se pudo conectar con la página del SIIF. Verifique su conexión")
        #     self.quit()

        try:
            await self.page.locator('id=pt1:it1::content').fill(self.username)
            await self.page.locator('id=pt1:it2::content').fill(self.password) 
            btn_connect = await self.page.locator('id=pt1:cb1')
            btn_connect.click()
        except Exception as e:
            print(f"Ocurrio un error: {e}")
            # print(f"No se encontro el elemento: {NoSuchElementException}")
            self.quit()

    # --------------------------------------------------
    async def go_to_reports(self) -> None:
        """"Go to SIIF's Reportes Module"""
        try:
            pass
            #Abrir pestaña reportes
            # self.wait.until(EC.presence_of_element_located(
            #     (By.XPATH, "//button[@id='pt1:cb12']"))
            # )
            # mnu_reportes = await self.page.locator('id=pt1:cb12')
            # mnu_reportes.click()
            # reportes = await self.page.locator('id=pt1:cb14')
            # reportes.click()
                    # Wait for the new window or tab
                    # self.wait.until(EC.number_of_windows_to_be(2))
                    # self.driver.switch_to.window(self.driver.window_handles[1])
                    # self.wait.until(EC.presence_of_element_located(
                    #     (By.XPATH, "//select[@id='pt1:socModulo::content']"))
                    # )
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
    def disconnect(self) -> None:
        pass
        # self.driver.switch_to.window(self.driver.window_handles[0])
        # btn_disconnect = self.driver.find_element(By.XPATH, "//a[@id='pt1:pt_np1:pt_cni1']")
        # btn_disconnect.click()
            # self.quit()

    # --------------------------------------------------
    def quit(self) -> None:
        # Quit
        self.driver.quit()