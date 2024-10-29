import os

import pytest
import pandas as pd

@pytest.mark.siif_rf610
@pytest.mark.usefixtures("setup_and_teardown_siif_rf610")
class TestSIIFRf610:
    def test_download_and_transform_rf610(self, tmpdir):
        try:
            dir_path = os.path.dirname(tmpdir)
            dir_path = os.path.join(dir_path, 'sub')
            print(f"El directorio temporal es: {dir_path}")
            # Llama al método download_report
            self.rf610.go_to_reports()
            self.rf610.download_report(
                dir_path, ejercicios=self.ejercicio
            )
            self.rf610.disconnect()
            # Verifica que se haya descargado el archivo correctamente
            report_path = os.path.join(dir_path, self.ejercicio + "-rf610.xls")
            assert os.path.exists(report_path), "No se descargó el archivo"

            # Llamar al método transform_df y pasar el DataFrame de prueba
            print(f"El archivo temporal es: {report_path}")
            transformed_df = self.rf610.from_external_report(report_path)
            # Crear un DataFrame esperado con la estructura deseada
            expected_df = pd.DataFrame({
                'ejercicio': [self.ejercicio],
                'estructura': ['11-00-02-79-421'],
                'programa': ['11'],
                'desc_prog': ['Programa de prueba'],
                'subprograma': ['00'],
                'desc_subprog': ['Subprograma de prueba'],
                'proyecto': ['02'],
                'desc_proy': ['Proyecto de prueba'],
                'actividad': ['79'],
                'desc_act': ['Actividad de prueba'],
                'grupo': ['400'],
                'desc_gpo': ['Grupo de prueba'],
                'partida': ['421'],
                'desc_part': ['Partida de prueba'],
                'credito_original': [500.0],
                'credito_vigente': [600.0],
                'comprometido': [700.0],
                'ordenado': [800.0],
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
            self.rf610.disconnect()
        else:
            assert True

# Ejecutar las pruebas
if __name__ == "__main__":
    pytest.main()