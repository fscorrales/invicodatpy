#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscorrales@gmail.com>
Purpose: Working With Files in Python
Source: https://realpython.com/working-with-files-in-python/#:~:text=To%20get%20a%20list%20of,scandir()%20in%20Python%203.
"""

import os
import pandas as pd

class HandlingFiles():

    # --------------------------------------------------
    def read_csv(self, PATH:str, names=None, header=None) -> pd.DataFrame:
        """"Read from csv SGF's report"""
        df = pd.read_csv(PATH, index_col=None, header=header, 
        na_filter = False, dtype=str, encoding = 'ISO-8859-1',
        on_bad_lines='warn', names=names)
        n_col = df.shape[1]
        df.columns = [str(x) for x in range(n_col)]
        return df

    # --------------------------------------------------
    def read_xls(self, PATH:str, header:int = None) -> pd.DataFrame:
        """"Read from xls report"""
        df = pd.read_excel(PATH, index_col=None, header=header, 
        na_filter = False, dtype=str)
        if header == None:
            n_col = df.shape[1]
            df.columns = [str(x) for x in range(n_col)]
        return df
    
    # --------------------------------------------------
    def get_list_of_files(self, path:str) -> list:
        """Get list of files in a folder
        :param path: folder or file.
        """
        # Check if path is a file and returns it. 
        # Otherwise returns list of files in folder.
        file_list = []
        if os.path.isfile(path):
            file_list.append(path)
        else:
            for entry in os.listdir(path):
                full_path = os.path.join(path, entry)
                if os.path.isfile(full_path):
                    file_list.append(full_path)
        return file_list