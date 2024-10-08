import os
import inspect

class HanglingPath():

    # --------------------------------------------------
    @classmethod
    def get_utils_path(self):
        dir_path = os.path.dirname(
            os.path.abspath(
                inspect.getfile(
                    inspect.currentframe())))
        return dir_path

    # --------------------------------------------------
    @classmethod
    def get_tests_path(self):
        dir_path = self.get_utils_path()
        dir_path = os.path.dirname(dir_path)
        return dir_path

    # --------------------------------------------------
    @classmethod
    def get_test_siif_path(self):
        dir_path = self.get_tests_path()
        dir_path = os.path.join(dir_path, 'siif')
        return dir_path

    # --------------------------------------------------
    @classmethod
    def get_test_sgf_path(self):
        dir_path = self.get_tests_path()
        dir_path = os.path.join(dir_path, 'sgf')
        return dir_path
    
    # --------------------------------------------------
    @classmethod
    def get_invicodatpy_path(self):
        dir_path = self.get_tests_path()
        dir_path = os.path.dirname(dir_path)
        return dir_path

    # --------------------------------------------------
    @classmethod
    def get_src_path(self):
        dir_path = self.get_invicodatpy_path()
        dir_path = os.path.join(dir_path, 'src')
        return dir_path

    # --------------------------------------------------
    @classmethod
    def get_siif_path(self):
        dir_path = self.get_src_path()
        dir_path = os.path.join(dir_path, 'invicodatpy', 'siif')
        return dir_path

    # --------------------------------------------------
    @classmethod
    def get_sgf_path(self):
        dir_path = self.get_src_path()
        dir_path = os.path.join(dir_path, 'invicodatpy', 'sgf')
        return dir_path
    
    # --------------------------------------------------
    @classmethod
    def get_r_icaro_path(self):
        dir_path = r'\\192.168.0.149\Compartida CONTABLE\R Apps (Compartida)\R Output\SQLite Files'
        return dir_path

    # --------------------------------------------------
    @classmethod
    def get_slave_path(self):
        dir_path = r'\\192.168.0.149\Compartida CONTABLE\Slave'
        return dir_path