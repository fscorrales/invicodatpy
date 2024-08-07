import pytest
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

@pytest.mark.usefixtures("setup_and_teardown_siif")
class TestSIIFConnection:
    def test_login_with_valid_credentials(self):
        try:
            self.connect_siif.connect(
                username=self.username,
                password=self.password
            )
        except Exception as e:
            assert False, f"Se lanzó una excepción: {e}"
        else:
            self.connect_siif.disconnect()
            assert True

    def test_login_without_entering_credentials(self):
        wait = WebDriverWait(self.connect_siif.driver, 10)
        try:
            self.connect_siif.connect()
            wait.until(EC.presence_of_element_located((
                By.XPATH, "//button[@id='pt1:cb12']"
            )))
        except TimeoutException:
            assert True
        else:
            assert False, "Acceso no autorizado sin credenciales"

    def test_access_to_reportes(self):
        wait = WebDriverWait(self.connect_siif.driver, 3)
        try:
            self.connect_siif.connect(
                username=self.username,
                password=self.password
            )
            self.connect_siif.go_to_reportes()
            wait.until(EC.presence_of_element_located((
                By.XPATH, "//select[@id='pt1:socModulo::content']"
            )))
        except TimeoutException:
            assert False, "No se pudo acceder a reportes"
            self.connect_siif.disconnect()
        else:
            assert True
            self.connect_siif.disconnect()

# Ejecutar las pruebas
if __name__ == "__main__":
    pytest.main()