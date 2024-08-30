import os

import pytest
import pandas as pd

@pytest.mark.sgf_rend_prov
@pytest.mark.usefixtures("setup_and_teardown_sgf_rend_prov")
class TestSGFRendProv:
    def test_download_and_transform_rend_prov(self, tmpdir):
        try:
            dir_path = os.path.dirname(tmpdir)
            dir_path = os.path.join(dir_path, 'sub')
            print(f"El directorio temporal es: {dir_path}")
            # Llama al método download_report
            self.connect_sgf.connect()
            self.sgf_rend_prov.download_report(
                dir_path, ejercicios=self.ejercicio, origenes=self.origenes
            )
            self.connect_sgf.quit()
            # Verifica que se haya descargado el archivo correctamente
            report_path = os.path.join(
                dir_path, self.ejercicio + ' Resumen de Rendiciones ' + 
                self.origenes[0] +".csv"
            )
            assert os.path.exists(report_path), "No se descargó el archivo"

            # Llamar al método transform_df y pasar el DataFrame de prueba
            print(f"El archivo temporal es: {report_path}")
            transformed_df = self.sgf_rend_prov.from_external_report(report_path)
            # Crear un DataFrame esperado con la estructura deseada
            expected_df = pd.DataFrame({
                'id': [1],
                'origen': [self.origenes[0]],
                'ejercicio': [self.ejercicio],
                'mes': ["01/" + self.ejercicio],
                'fecha': [pd.to_datetime('today').normalize()],
                'beneficiario': ['INVICO'],
                'destino': ['HONORARIOS - EPAM'],
                'libramiento_sgf':['12345'], 
                'movimiento': ['TRANSF.'],
                'cta_cte': ['130832-07'],
                'importe_bruto': [700.0],
                'gcias': [0.0],
                'sellos': [0.0],
                'iibb': [0.0],
                'suss': [0.0],
                'invico': [0.0],
                'seguro': [0.0],
                'salud': [0.0],
                'mutual': [0.0],
                'otras': [0.0],
                'retenciones': [0.0],
                'importe_neto': [700.0],
                'saldo': [700.0],
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
            self.connect_sgf.quit()
        else:
            assert True

# Ejecutar las pruebas
if __name__ == "__main__":
    pytest.main()