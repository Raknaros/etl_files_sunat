import os
import zipfile
import logging
import numpy as np
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine, text
from typing import List, Optional

from app.config import config, COLUMN_MAPPING_VENTAS

# Configuración de logging
logger = logging.getLogger(__name__)

class Extractor:
    @staticmethod
    def extract_files(rutas_archivos: List[str]) -> List[pd.DataFrame]:
        lista_dataframes = []
        logger.info("Iniciando fase de extracción para SIRE Ventas")

        for ruta in rutas_archivos:
            try:
                logger.info(f"Procesando archivo: {os.path.basename(ruta)}")
                if ruta.lower().endswith('.zip'):
                    with zipfile.ZipFile(ruta, 'r') as zip_ref:
                        for nombre_archivo in zip_ref.namelist():
                            if nombre_archivo.lower().endswith('.txt'):
                                with zip_ref.open(nombre_archivo) as file:
                                    content = file.read().decode('latin-1', errors='replace')
                                    # CORRECCIÓN: Usar header=0 para leer el encabezado del archivo
                                    df = pd.read_csv(StringIO(content), sep='|', header=0, dtype=str)
                                    lista_dataframes.append(df)
                elif ruta.lower().endswith('.txt'):
                    # CORRECCIÓN: Usar header=0 para leer el encabezado del archivo
                    df = pd.read_csv(ruta, sep='|', header=0, dtype=str, encoding='latin-1')
                    lista_dataframes.append(df)
            except pd.errors.EmptyDataError:
                logger.warning(f"Archivo omitido: '{os.path.basename(ruta)}' no contiene datos o columnas.")
            except Exception as e:
                logger.error(f"Error al procesar '{os.path.basename(ruta)}': {e}")

        return lista_dataframes


class Transformer:
    @staticmethod
    def transform_data(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Iniciando fase de transformación de SIRE Ventas")
        df_transformado = df.copy()

        # Limpiar nombres de columnas de espacios extra
        df_transformado.columns = df_transformado.columns.str.strip()

        if 'CAR SUNAT' in df_transformado.columns:
            df_transformado['CAR SUNAT'] = df_transformado['CAR SUNAT'].astype(str)
            before_count = len(df_transformado)
            df_transformado = df_transformado[df_transformado['CAR SUNAT'].str.len().isin([27, 29]) | (df_transformado['CAR SUNAT'] == '') | (df_transformado['CAR SUNAT'].isnull())]
            after_count = len(df_transformado)
            logging.info(f"Filtro CAR SUNAT (Ventas): {before_count} -> {after_count} filas")

        if 'Tipo Doc Identidad' in df_transformado.columns and 'Nro Doc Identidad' in df_transformado.columns and 'Apellidos Nombres/ Razón Social' in df_transformado.columns:
            tipo_doc_mask = df_transformado['Tipo Doc Identidad'] == '-'
            df_transformado.loc[tipo_doc_mask, 'Tipo Doc Identidad'] = '0'
            nro_doc_mask = (df_transformado['Tipo Doc Identidad'] == '0') & (df_transformado['Nro Doc Identidad'] == '-')
            df_transformado.loc[nro_doc_mask, 'Nro Doc Identidad'] = df_transformado.loc[nro_doc_mask, 'Apellidos Nombres/ Razón Social']
            logging.info(f"Regla especial de documentos aplicada.")

        date_columns = ['Fecha de emisión', 'Fecha Vcto/Pago']
        for col in date_columns:
            if col in df_transformado.columns:
                df_transformado[col] = pd.to_datetime(df_transformado[col], format='%d/%m/%Y', errors='coerce').dt.date

        if 'Periodo' in df_transformado.columns:
            df_transformado['Periodo'] = pd.to_datetime(df_transformado['Periodo'], format='%Y%m', errors='coerce')

        columnas_monto = ['BI Gravada', 'Dscto BI', 'IGV / IPM', 'Dscto IGV / IPM',
                          'Mto Exonerado', 'Mto Inafecto', 'BI Grav IVAP', 'IVAP',
                          'ISC', 'ICBPER', 'Otros Tributos', 'Valor Facturado Exportación']
        for col in columnas_monto:
            if col in df_transformado.columns:
                df_transformado[col] = pd.to_numeric(df_transformado[col], errors='coerce').fillna(0)

        if 'Tipo Doc Identidad' in df_transformado.columns:
            df_transformado['Tipo Doc Identidad'] = df_transformado['Tipo Doc Identidad'].replace('-', '0')
            df_transformado['Tipo Doc Identidad'] = pd.to_numeric(df_transformado['Tipo Doc Identidad'], errors='coerce')

        Transformer._aplicar_filtro_complejo(df_transformado)
        logging.info(f"Transformación de SIRE Ventas completada: {len(df_transformado)} filas")
        return df_transformado
    
    @staticmethod
    def _aplicar_filtro_complejo(df: pd.DataFrame) -> None:
        logger.info("Aplicando filtro complejo de negocio para SIRE Ventas.")
        columnas_valor = [
            'BI Gravada', 'Dscto BI', 'IGV / IPM', 'Dscto IGV / IPM',
            'Mto Exonerado', 'Mto Inafecto', 'BI Grav IVAP', 'IVAP',
            'Otros Tributos', 'Valor Facturado Exportación', 'Tipo CP/Doc.'
        ]
        for col in columnas_valor:
            if col not in df.columns: df[col] = 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        suma_exo_inaf = df['Mto Exonerado'] + df['Mto Inafecto']
        condiciones = [
            (df['Tipo CP/Doc.'] == 7) & (df['Valor Facturado Exportación'] < 0),
            (df['Tipo CP/Doc.'] == 7) & (df['Valor Facturado Exportación'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] > 0) & (df['BI Gravada'] == 0) & (df['Dscto BI'] == 0) & (df['IGV / IPM'] == 0) & (df['Dscto IGV / IPM'] == 0) & (df['Mto Exonerado'] == 0) & (df['Mto Inafecto'] == 0) & (df['BI Grav IVAP'] == 0) & (df['IVAP'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] > 0) & (df['IGV / IPM'] > 0) & (suma_exo_inaf > 0) & (df['BI Grav IVAP'] == 0) & (df['IVAP'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] > 0) & (df['IGV / IPM'] > 0) & (suma_exo_inaf == 0) & (df['BI Grav IVAP'] == 0) & (df['IVAP'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] == 0) & (df['Dscto BI'] == 0) & (df['IGV / IPM'] == 0) & (df['Dscto IGV / IPM'] == 0) & (suma_exo_inaf > 0) & (df['BI Grav IVAP'] == 0) & (df['IVAP'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] == 0) & (df['Dscto BI'] == 0) & (df['IGV / IPM'] == 0) & (df['Dscto IGV / IPM'] == 0) & (suma_exo_inaf == 0) & (df['BI Grav IVAP'] > 0) & (df['IVAP'] > 0)
        ]
        resultados_tipo_op = [1, 1, 17, 1, 1, 1, 1]
        resultados_destino = [1, 1, 2, 3, 1, 2, 4]
        resultados_valor = [
            df['BI Gravada'] + df['Dscto BI'] + df['BI Grav IVAP'], df['Valor Facturado Exportación'], df['Valor Facturado Exportación'],
            df['BI Gravada'], df['BI Gravada'], suma_exo_inaf, df['BI Grav IVAP']
        ]
        resultados_igv = [
            df['IGV / IPM'] + df['Dscto IGV / IPM'] + df['IVAP'], 0, 0, df['IGV / IPM'], df['IGV / IPM'], 0, df['IVAP']
        ]
        resultados_otros = [
            df['Otros Tributos'], df['Otros Tributos'], df['Otros Tributos'], df['Otros Tributos'] + suma_exo_inaf,
            df['Otros Tributos'], df['Otros Tributos'], df['Otros Tributos'] + suma_exo_inaf
        ]

        df['tipo_operacion'] = np.select(condiciones, resultados_tipo_op, default=99)
        df['destino'] = np.select(condiciones, resultados_destino, default=99)
        df['valor'] = np.select(condiciones, resultados_valor, default=0)
        df['igv'] = np.select(condiciones, resultados_igv, default=0)
        df['otros_cargos'] = np.select(condiciones, resultados_otros, default=df['Otros Tributos'])

        if 'CAR SUNAT' in df.columns:
            df.loc[df['destino'] == 99, 'CAR SUNAT'] = df['CAR SUNAT'].astype(str) + " | Revisar dinamica de destino"
        logger.info("Lógica de negocio compleja aplicada.")

    @staticmethod
    def rename_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
        return df.rename(columns=mapping)

    @staticmethod
    def filter_final_columns(df: pd.DataFrame) -> pd.DataFrame:
        if 'observaciones' in df.columns:
            df['observaciones'] = "SIRE:" + df['observaciones'].astype(str)

        columnas_finales = [
            'ruc', 'periodo_tributario', 'tipo_comprobante', 'fecha_emision',
            'fecha_vencimiento', 'numero_serie', 'numero_correlativo', 'numero_final', 'tipo_documento',
            'numero_documento', 'destino', 'valor', 'igv', 'icbp', 'isc', 'otros_cargos',
            'tipo_moneda', 'tipo_comprobante_modificado','numero_serie_modificado',
            'numero_correlativo_modificado', 'observaciones', 'tipo_operacion'
        ]
        columnas_existentes = [col for col in columnas_finales if col in df.columns]
        df_filtrado = df[columnas_existentes].copy()
        df_filtrado = df_filtrado.replace({'': np.nan, ' ': np.nan, 'nan': np.nan})
        Transformer._convert_data_types(df_filtrado)
        return df_filtrado

    @staticmethod
    def _convert_data_types(df: pd.DataFrame) -> None:
        if 'ruc' in df.columns: df['ruc'] = pd.to_numeric(df['ruc'], errors='coerce').astype('Int64')
        int_columns = ['tipo_comprobante', 'destino', 'tasa_detraccion', 'tipo_comprobante_modificado', 'numero_final', 'tipo_operacion']
        for col in int_columns:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        if 'periodo_tributario' in df.columns:
            df['periodo_tributario'] = pd.to_datetime(df['periodo_tributario'], errors='coerce').dt.strftime('%Y%m')
            df['periodo_tributario'] = pd.to_numeric(df['periodo_tributario'], errors='coerce').astype('Int64')

        date_columns = ['fecha_emision', 'fecha_vencimiento']
        for col in date_columns:
            if col in df.columns: df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        numeric_cols = ['valor', 'igv', 'icbp', 'isc', 'otros_cargos']
        for col in numeric_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').round(2)


class Loader:
    def __init__(self, db_url: str, schema: str, table: str):
        self.engine = create_engine(db_url)
        self.schema = schema
        self.table = table
        self.full_table_name = f"{self.schema}.{self.table}"

    def load_data(self, df: pd.DataFrame) -> bool:
        logger.info(f"Iniciando carga de {len(df)} filas a {self.full_table_name}")
        insert_count = 0
        error_count = 0
        df_prepared = df.replace({np.nan: None})

        with self.engine.connect() as connection:
            with connection.begin() as transaction:
                for index, row in df_prepared.iterrows():
                    savepoint = connection.begin_nested()
                    try:
                        columns = ', '.join(row.index)
                        placeholders = ', '.join([f":{col}" for col in row.index])
                        stmt = text(f"INSERT INTO {self.full_table_name} ({columns}) VALUES ({placeholders})")
                        connection.execute(stmt, row.to_dict())
                        savepoint.commit()
                        insert_count += 1
                    except Exception as e:
                        savepoint.rollback()
                        error_count += 1
                        logger.error(f"Error al insertar fila {index} en {self.full_table_name}: {e}")
                        logger.debug(f"Datos de la fila con error: {row.to_dict()}")
        
        logger.info(f"Carga completada: {insert_count} filas insertadas, {error_count} errores.")
        return error_count == 0


class ETLSIRE:
    def __init__(self, db_url: str, schema: str, table: str, column_mapping: Optional[dict] = None):
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader(db_url, schema, table)
        self.column_mapping = column_mapping or {}

    def run(self, rutas_archivos: List[str], show_preview: bool = False) -> bool:
        try:
            dataframes = self.extractor.extract_files(rutas_archivos)
            if not dataframes:
                logger.warning("No se extrajeron datos válidos de ningún archivo.")
                return True
            
            df_completo = pd.concat(dataframes, ignore_index=True)
            logger.info(f"Total de filas extraídas de todos los archivos: {len(df_completo)}")

            df_renamed = self.transformer.rename_columns(df_completo, self.column_mapping)
            df_transformed = self.transformer.transform_data(df_renamed)
            df_final = self.transformer.filter_final_columns(df_transformed)
            
            if show_preview:
                pd.set_option('display.max_columns', None)
                pd.set_option('display.max_rows', None)
                print("=== PREVIEW DEL DATAFRAME FINAL (SIRE VENTAS) ===")
                print(df_final.head())
                print(f"Total de filas a cargar: {len(df_final)}")
                print("=" * 50)

            success = self.loader.load_data(df_final)
            return success

        except Exception as e:
            logger.critical(f"Error fatal en el proceso ETL de SIRE Ventas: {str(e)}", exc_info=True)
            return False


def run_sire_ventas_etl(file_paths: List[str], show_preview: bool = False) -> bool:
    logger.info(f"Iniciando ETL de SIRE Ventas para {len(file_paths)} archivo(s).")
    db_url = config.DB_URL
    schema = "acc"
    table = "_5" 

    etl = ETLSIRE(db_url, schema, table, COLUMN_MAPPING_VENTAS)
    success = etl.run(file_paths, show_preview=show_preview)

    if success:
        logger.info("ETL de SIRE Ventas completado exitosamente.")
    else:
        logger.warning("ETL de SIRE Ventas finalizado con errores.")
        
    return success