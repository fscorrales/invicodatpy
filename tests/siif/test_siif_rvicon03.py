import os

import pytest
import pandas as pd

@pytest.mark.siif_rvicon03
@pytest.mark.usefixtures("setup_and_teardown_siif_rvicon03")
class TestSIIFRvicon03:
    def test_download_and_transform_rvicon03(self, tmpdir):
        try:
            dir_path = os.path.dirname(tmpdir)
            dir_path = os.path.join(dir_path, 'sub')
            print(f"El directorio temporal es: {dir_path}")
            # Llama al método download_report
            self.connect_siif.connect()
            self.connect_siif.go_to_reports()
            self.resumen_contable_cta_rvicon03.download_report(
                dir_path, ejercicios=self.ejercicio
            )
            self.connect_siif.disconnect()
            # Verifica que se haya descargado el archivo correctamente
            report_path = os.path.join(dir_path, self.ejercicio + "-rvicon03.xls")
            assert os.path.exists(report_path), "No se descargó el archivo"

            # Llamar al método transform_df y pasar el DataFrame de prueba
            print(f"El archivo temporal es: {report_path}")
            transformed_df = self.resumen_contable_cta_rvicon03.from_external_report(report_path)
            # Crear un DataFrame esperado con la estructura deseada
            expected_df = pd.DataFrame({
                'ejercicio': [self.ejercicio],
                'nivel': ['1000'],
                'nivel_desc': ['ACTIVOS'],
                'cta_contable': ['1112-2-6'],
                'cta_contable_desc': ['CUENTAS PAGADORAS Y '],
                'saldo_inicial': [100.0],
                'debe': [200.0],
                'haber': [300.0],
                'ajuste_debe': [400.0],
                'ajuste_haber': [500.0],
                'fondos_debe': [600.0],
                'fondos_haber': [700.0],
                'saldo_final': [800.0]
            })

            # Comparar el número de columnas
            num_columns_equal = len(transformed_df.columns) == len(expected_df.columns)
            if not num_columns_equal:
                assert False, "El número de columnas no coincide"

            # Verificar que los nombres de las columnas sean iguales
            column_names_equal = list(transformed_df.columns) == list(expected_df.columns)
            if not column_names_equal:
                assert False, "Los nombres de las columnas no coinciden"

            # Verificar que los tipos de datos de las columnas sean iguales
            dtypes_equal = all(transformed_df.dtypes == expected_df.dtypes)
            if not dtypes_equal:
                assert False, "Los tipos de datos de las columnas no coinciden"

        except Exception as e:
            assert False, f"Se lanzó una excepción: {e}"
            self.connect_siif.disconnect()
        else:
            assert True

# Ejecutar las pruebas
if __name__ == "__main__":
    pytest.main()