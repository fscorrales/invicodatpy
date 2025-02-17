#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Web Scrapping SGV with selenium
Source: 
"""

import argparse
import inspect
import json
import os
import glob
import sys
import time
from dataclasses import dataclass, field

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


@dataclass
class ConnectSGO():
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

        options.add_experimental_option('prefs', {
            # "download.default_directory": "C:/Users/XXXX/Desktop", #Change default directory for downloads
            "download.prompt_for_download": False, #To auto download the file
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True #It will not show PDF directly in chrome
        })
        driver_path = ChromeDriverManager().install()
        self.driver = webdriver.Chrome(executable_path=driver_path, options=options)
        # self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        self.driver.maximize_window()

        # Setup wait for later
        self.wait = WebDriverWait(self.driver, 100)
        
        self.connect()
        # self.go_to_reports()

    # --------------------------------------------------
    def connect(self) -> None:
        """"Connect SGV"""
        self.driver.get('https://obras.invico.gov.ar/Account/Login')
        try:
            self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[@class='login']")))
            input_username = self.driver.find_element(By.XPATH, "//input[@id='UserName']")
            input_password = self.driver.find_element(By.XPATH, "//input[@id='Password']")
            btn_connect = self.driver.find_element(By.XPATH, "//*[@id='formConEstilo']/form/button")
            input_username.send_keys(self.username)
            input_password.send_keys(self.password) 
            btn_connect.click()
            time.sleep(2)
            self.wait.until(EC.presence_of_element_located((
                By.XPATH, "//*[@id='grillaObras']/tbody/tr[2]"
            )))
        except Exception as e:
            print(f"Ocurrió un error: {e}, {type(e)}")
            self.quit()

    # --------------------------------------------------
    def rename_report(self, dir_path:str, old_name:str, new_name:str):
        old_file_path = glob.glob(os.path.join(dir_path, old_name))
        while not os.path.exists(old_file_path[0]):
            time.sleep(1)

        old_file_path = old_file_path[0]
        new_file_path = os.path.join(dir_path, new_name)

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
        time.sleep(5)
        file_list = os.listdir(dir_path)
        for f in file_list:
            if '.htm' in f:
                file_path = os.path.join(dir_path, f)
                os.remove(file_path)
                print(f"File: {file_path} removed")
        # root_dir = dir_path
        # for folder, subfolders, files in os.walk(root_dir):
        #     if folder != root_dir:
        #         print(f"Folder: {folder}")
        #         print(f"Files: {files}")
        #         for f in files:
        #             # https://stackoverflow.com/questions/33743816/how-to-find-a-filename-that-contains-a-given-string
        #             if 'htm' in f:
        #                 file_path = os.path.join(folder, f)
        #                 os.remove(file_path)
        #                 print(f"File: {file_path} removed")
        #             if 'crdownload' in f:
        #                 file_path = os.path.join(folder, f)
        #                 os.remove(file_path)
        #                 print(f"File: {file_path} removed")

    # --------------------------------------------------
    def disconnect(self) -> None:
        # self.driver.switch_to.window(self.driver.window_handles[0])
        mnu_popup = self.driver.find_element(
            By.XPATH, "//*[@id='header']/ul[2]/li/div"
        )
        mnu_popup.click()
        time.sleep(1)
        btn_disconnect = self.driver.find_element(
            By.XPATH, "//*[@id='header']/ul[2]/li/div/ul/li[3]/a"
        )
        btn_disconnect.click()
        time.sleep(1)
        self.driver.refresh()
        time.sleep(1)
        self.quit()

    # --------------------------------------------------
    def quit(self) -> None:
        # Quit
        self.driver.quit()

# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description = "Try connect SGV with selenium",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-u', '--username', 
        metavar = 'username',
        default = '',
        type=str,
        help = "Username to connect to GV")

    parser.add_argument(
        '-p', '--password', 
        metavar = 'password',
        default = '',
        type=str,
        help = "Password to connect to GV")

    return parser.parse_args()

# --------------------------------------------------
def main():
    """Let's try it"""
    args = get_args()
    dir_path = os.path.dirname(
        os.path.abspath(
            inspect.getfile(
                inspect.currentframe())))
    
    json_path = dir_path + '/credentials.json'
    print(json_path)

    if args.username != '' and args.password != '':
        sgv = ConnectSGO(
            username = args.username,
            password=args.password,
        )
    else:
        if os.path.isfile(json_path):
            with open(json_path) as json_file:
                data_json = json.load(json_file)
                sgv = ConnectSGO(
                    username=data_json['username'],
                    password= data_json['password'],
                )
            json_file.close()
        else:
            msg = (
                f'If {json_path} password ' +
                'as key does not exist in the directory, ' + 
                'it must be given.'
            )
            sys.exit(msg)

    sgv.disconnect()

# --------------------------------------------------
if __name__ == '__main__':
    main()
    # From invicodatpy/src
    # python -m invicodatpy.sgo.connect_sgo
