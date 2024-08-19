import os

import pytest
import pandas as pd

@pytest.mark.siif_rdeu012
@pytest.mark.usefixtures("setup_and_teardown_siif_rdeu012")
class TestSIIFRf610:
    def test_download_and_transform_rdeu012(self, tmpdir):
        try:
            dir_path = os.path.dirname(tmpdir)
            dir_path = os.path.join(dir_path, 'sub')
            print(f"El directorio temporal es: {dir_path}")
            # Llama al método download_report
            self.connect_siif.connect()
            self.connect_siif.go_to_reports()
            self.rdeu012.download_report(
                dir_path, meses=self.ejercicio_mes
            )
            self.connect_siif.disconnect()
            # Verifica que se haya descargado el archivo correctamente
            report_path = os.path.join(dir_path, self.ejercicio_mes + "-rdeu012.xls")
            assert os.path.exists(report_path), "No se descargó el archivo"

            # Llamar al método transform_df y pasar el DataFrame de prueba
            print(f"El archivo temporal es: {report_path}")
            transformed_df = self.rdeu012.from_external_report(report_path)
            # Crear un DataFrame esperado con la estructura deseada
            expected_df = pd.DataFrame({
                'ejercicio': [self.ejercicio_mes[0:4]],
                'mes': ['01/2024'],
                'fecha': [pd.to_datetime('today').normalize()],
                'mes_hasta': ['01/2024'],
                'fuente': ['11'],
                'cta_cte': ['130832-13'],
                'nro_comprobante': ['02525/24'],
                'importe': [700.0],
                'saldo': [700.0],
                'cuit': ['123456789'],
                'beneficiario': ['CALLE 1'],
                'glosa': ['Esto es un texto un poco largo'],
                'nro_expte': ['123456789'],
                'nro_entrada': ['2525'],
                'nro_origen': ['2525'],
                'fecha_aprobado': [pd.to_datetime('today').normalize()],
                'fecha_desde': [pd.to_datetime('today').normalize()],
                'fecha_hasta': [pd.to_datetime('today').normalize()],
                'org_fin': ['01'],
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