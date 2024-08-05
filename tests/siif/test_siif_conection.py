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
            wait.until(EC.presence_of_element_located((By.XPATH, "//button[@id='pt1:cb12']")))
        except TimeoutException:
            assert True
        else:
            assert False, "No se lanzó la excepción TimeoutException"
        # with pytest.raises(TimeoutException):
        #     self.connect_siif.connect()
        # try:
        #     self.connect_siif.connect()
        # except Exception as e:
        #     print(f"Se lanzó la excepción: {e}")
        #     assert isinstance(e, TimeoutException)
        # else:
        #     assert False, "No se lanzó la excepción TimeoutException"

def wait_until_element_present(driver, locator):
    wait = WebDriverWait(driver, 10)
    try:
        wait.until(EC.presence_of_element_located(locator))
    except TimeoutException:
        assert False

# Ejecutar las pruebas
if __name__ == "__main__":
    pytest.main()