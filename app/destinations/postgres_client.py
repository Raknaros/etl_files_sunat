# Cliente para PostgreSQL - Implementado con estrategias de verificación

import psycopg2
from app.config import config, VERIFICATION_STRATEGIES, generar_identificador_procesamiento

class PostgresClient:
    def __init__(self):
        self.connection_params = {
            'host': config.POSTGRES_HOST,
            'port': config.POSTGRES_PORT,
            'database': config.POSTGRES_DB,
            'user': config.POSTGRES_USER,
            'password': config.POSTGRES_PASSWORD
        }
        self._connection = None

    def _get_connection(self):
        """Obtiene conexión a PostgreSQL."""
        if not self._connection:
            self._connection = psycopg2.connect(**self.connection_params)
        return self._connection

    def check_file_processed(self, file_name):
        """
        Verifica procesamiento según estrategia del tipo de archivo.
        """
        from app.config import match_file_pattern

        tipo, data, _ = match_file_pattern(file_name)
        if not tipo or tipo not in VERIFICATION_STRATEGIES:
            return False

        strategy = VERIFICATION_STRATEGIES[tipo]
        method = strategy["method"]

        if method == "single_row_check":
            return self._check_single_row(strategy, data)
        elif method == "row_by_row_check":
            return False  # Siempre procesar, verificar internamente
        elif method == "timestamp_check":
            return self._check_timestamp(strategy, data, file_name)

        return False

    def _check_single_row(self, strategy, data):
        """Verificación de archivos que corresponden a una sola fila."""
        table = strategy["table"]
        id_value = self._build_identifier_value(strategy["id_column"], data)

        query = f"""
            SELECT 1 FROM {table}
            WHERE {strategy["id_column"]} = %s
            AND {strategy["check_column"]} = %s
        """

        try:
            with self._get_connection().cursor() as cursor:
                cursor.execute(query, (id_value, strategy["check_value"]))
                return cursor.fetchone() is not None
        except Exception as e:
            print(f"Error en verificación single_row: {e}")
            return False

    def _check_timestamp(self, strategy, data, file_name):
        """Verificación por timestamp - solo procesar si es más reciente."""
        identificador = generar_identificador_procesamiento(strategy.get("tipo", ""), data)
        timestamp_actual = int(data.get("timestamp", 0))

        # Verificar si existe una versión más reciente procesada
        query = """
            SELECT 1 FROM archivos_procesados
            WHERE identificador = %s
            AND timestamp_archivo >= %s
            AND estado = 'PROCESADO'
        """

        try:
            with self._get_connection().cursor() as cursor:
                cursor.execute(query, (identificador, timestamp_actual))
                exists_newer = cursor.fetchone() is not None
                return exists_newer  # True si ya hay versión más reciente
        except Exception as e:
            print(f"Error en verificación timestamp: {e}")
            return False

    def _build_identifier_value(self, id_expression, data):
        """Construye el valor del identificador basado en la expresión SQL."""
        # Para expresiones simples como "numero_serie || '-' || numero_correlativo"
        # Asumimos que los campos están en data
        if "numero_serie" in id_expression and "numero_correlativo" in id_expression:
            serie = data.get("serie", "")
            correlativo = data.get("correlativo", "")
            return f"{serie}-{correlativo}"
        # Agregar más lógica según necesidad
        return ""

    def registrar_procesamiento(self, tipo, identificador, nombre_archivo, ruc, timestamp_archivo):
        """Registra archivo procesado en tabla de control."""
        query = """
            INSERT INTO archivos_procesados
            (tipo_documento, identificador, nombre_archivo, ruc, timestamp_archivo, estado)
            VALUES (%s, %s, %s, %s, %s, 'PROCESADO')
            ON CONFLICT (tipo_documento, identificador)
            DO UPDATE SET
                nombre_archivo = EXCLUDED.nombre_archivo,
                fecha_procesamiento = CURRENT_TIMESTAMP,
                timestamp_archivo = EXCLUDED.timestamp_archivo
        """

        try:
            with self._get_connection().cursor() as cursor:
                cursor.execute(query, (tipo, identificador, nombre_archivo, ruc, timestamp_archivo))
                self._get_connection().commit()
        except Exception as e:
            print(f"Error registrando procesamiento: {e}")

    def insert_data(self, table, data):
        """
        Inserta datos en la tabla especificada.
        data: lista de dicts con columnas y valores.
        """
        if not data:
            return

        columns = list(data[0].keys())
        values_placeholder = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values_placeholder})"

        try:
            with self._get_connection().cursor() as cursor:
                for row in data:
                    values = [row[col] for col in columns]
                    cursor.execute(query, values)
                self._get_connection().commit()
        except Exception as e:
            print(f"Error insertando datos: {e}")
            self._get_connection().rollback()

# Instancia
postgres_client = PostgresClient()