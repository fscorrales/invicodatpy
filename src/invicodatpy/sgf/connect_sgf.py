#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Automate SGF with pywinauto
Source: 
"""

__all__ = ['ConnectSGF']

import os
import time
from dataclasses import dataclass

from pywinauto import keyboard
from pywinauto.application import Application
from pywinauto.timings import TimeoutError


@dataclass
class ConnectSGF():
    username:str = '' 
    password:str = ''

    # --------------------------------------------------
    # def __post_init__(self):        
    #     self.connect()
        # self.go_to_reports()

    # --------------------------------------------------
    def connect(self, username:str = '', password:str = '') -> None:
        if username != '':
            self.username = username
        if password !='':
            self.password = password

        """"Connect SGF"""
        app_path = r"\\ipvfiles\SISTEMAS\Pagos\Pagos.exe"
        self.app = Application(backend='uia').start(app_path)
        try:
            time.sleep(3)
            self.main = self.app.window(title_re=".*Sistema de Gestión Financiera.*")
            if not self.main.is_maximized():
                self.main.maximize()
            cmb_user = self.main.child_window(
                auto_id="1", control_type="ComboBox", found_index = 0
            ).wait('exists enabled visible ready')
            input_password = self.main.child_window(
                auto_id="2", control_type="Edit", found_index = 0
            ).wrapper_object()
            cmb_user.type_keys(self.username)
            input_password.type_keys(self.password)
            btn_accept = self.main.child_window(
                title="Aceptar", auto_id="4", control_type="Button"
            ).wrapper_object()
            btn_accept.click()
            self.main.child_window(
            title="La contraseña no es válida. Vuelva a intentarlo", 
            auto_id="65535", control_type="Text").wait_not('exists visible enabled', timeout=1)
        except TimeoutError:
            print("No se pudo conectar al SGF. Verifique sus credenciales")
            close_button = self.main.child_window(title="Cerrar", control_type="Button", found_index=0)
            close_button.click()
            btn_cancel = self.main.child_window(
                title="Cancelar", auto_id="3", control_type="Button"
            ).wrapper_object()
            btn_cancel.click()
        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.quit()

    # --------------------------------------------------
    def move_report(self, dir_path:str, name:str):
        old_file_path = os.path.join(r"D:\Users\fcorrales\Desktop", name)
        new_file_path = os.path.join(dir_path, name)
        while not os.path.exists(old_file_path):
            time.sleep(1)
            while self.is_locked(old_file_path):
                time.sleep(1)

        if os.path.isfile(old_file_path):
            if os.path.isfile(new_file_path):
                os.remove(new_file_path)
            os.rename(old_file_path, new_file_path)
        else:
            raise ValueError("%s isn't a file!" % old_file_path)

    # --------------------------------------------------
    @classmethod
    def is_locked(cls, filepath):
        """Checks if a file is locked by opening it in append mode.
        If no exception thrown, then the file is not locked.
        """
        locked = None
        file_object = None
        if os.path.exists(filepath):
            try:
                buffer_size = 8
                # Opening file in append mode and read the first 8 characters.
                file_object = open(filepath, 'a', buffer_size)
                if file_object:
                    locked = False
            except IOError:
                locked = True
            finally:
                if file_object:
                    file_object.close()
        else:
            print("%s not found." % filepath)
        return locked

    # --------------------------------------------------
    @classmethod
    def is_alive(self):
        # Check if the app is alive
        try:
            self.app.process
        except Exception:
            raise RuntimeError("The app is not alive")
            return False
        return True

    # --------------------------------------------------
    def quit(self) -> None:
        # Quit
        keyboard.send_keys('%s')
