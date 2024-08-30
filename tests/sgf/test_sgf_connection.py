import pytest

@pytest.mark.usefixtures("setup_and_teardown_sgf")
class TestSGFConnection:
    @pytest.mark.sgf_login
    def test_login_with_valid_credentials(self):
        print("Voy a conectar SGF")
        try:
            self.connect_sgf.connect(
                username=self.username,
                password=self.password
            )
            print("Me conecté SGF")
        except Exception as e:
            assert False, f"Se lanzó una excepción: {e}"

    def test_login_without_entering_credentials(self):
        with pytest.raises(RuntimeError):
            self.connect_sgf.connect()
            assert not (
                self.connect_sgf.is_alive(), 
                "El objeto de conexión no debería estar vivo después de un error de inicio de sesión"
            )

# Ejecutar las pruebas
if __name__ == "__main__":
    pytest.main()