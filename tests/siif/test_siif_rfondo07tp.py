import os

import pytest
import pandas as pd

@pytest.mark.siif_rfondo07tp
@pytest.mark.usefixtures("setup_and_teardown_siif_rfondo07tp")
class TestSIIFRfondo07tp:
    def test_download_and_transform_rfondo07tp(self, tmpdir):
        try:
            dir_path = os.path.dirname(tmpdir)
            dir_path = os.path.join(dir_path, 'sub')
            print(f"El directorio temporal es: {dir_path}")
            # Llama al método download_report
            self.connect_siif.connect()
            self.connect_siif.go_to_reports()
            self.rfondo07tp.download_report(
                dir_path, ejercicios=self.ejercicio, tipo_comprobante='PA6'
            )
            self.connect_siif.disconnect()
            # Verifica que se haya descargado el archivo correctamente
            report_path = os.path.join(dir_path, self.ejercicio + "-rfondo07tp (PA6).xls")
            assert os.path.exists(report_path), "No se descargó el archivo"

            # Llamar al método transform_df y pasar el DataFrame de prueba
            print(f"El archivo temporal es: {report_path}")
            transformed_df = self.rfondo07tp.from_external_report(report_path)
            # Crear un DataFrame esperado con la estructura deseada
            expected_df = pd.DataFrame({
                'ejercicio': [self.ejercicio],
                'mes': ['12/2024'],
                'fecha': [pd.to_datetime('today').normalize()],
                'tipo_comprobante': ['PA6'],
                'nro_comprobante': ['00001/24'],
                'nro_fondo': ['0'],
                'glosa': ['Un texto con mucho bla bla'],
                'ingresos': [500.0],
                'egresos': [500.0],
                'saldo': [500.0],
            })

            # Comparar el número de columnas
            num_columns_equal = len(transformed_df.columns) == len(expected_df.columns)
            if not num_columns_equal:
                print("Número de columnas obtenidas:\n")
                print(len(transformed_df.columns))
                print("Número de columnas esperadas:\n")
                print(len(expected_df.columns))
                assert False, "El número de columnas no coincide"

            # Verificar que los nombres de las columnas sean iguales
            column_names_equal = list(transformed_df.columns) == list(expected_df.columns)
            if not column_names_equal:
                print("Nombres de las columnas obtenidas:\n")
                print(list(transformed_df.columns))
                print("Nombres de las columnas esperadas:\n")
                print(list(expected_df.columns))
                assert False, "Los nombres de las columnas no coinciden"

            # Verificar que los tipos de datos de las columnas sean iguales
            dtypes_equal = all(transformed_df.dtypes == expected_df.dtypes)
            if not dtypes_equal:
                print("Tipos de datos de las columnas obtenidas:\n")
                print(transformed_df.dtypes)
                print("Tipos de datos de las columnas esperadas:\n")
                print(expected_df.dtypes)
                assert False, "Los tipos de datos de las columnas no coinciden"

        except Exception as e:
            assert False, f"Se lanzó una excepción: {e}"
            self.connect_siif.disconnect()
        else:
            assert True

# Ejecutar las pruebas
if __name__ == "__main__":
    pytest.main()