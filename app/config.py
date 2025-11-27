import os
import re
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Patrones regex para los nombres estructurados de archivos SUNAT
PATRONES_NO_ETL = {
    "ficha_ruc": (re.compile(r"^reporteec_ficharuc_(\d{11})_(\d{14})\.(pdf)$", re.IGNORECASE), ["ruc", "timestamp", "ext"]),
    "ingreso_recaudacion": (re.compile(r"^ridetrac_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "resolucion", "timestamp", "id", "ext"]),
    "liberacion_fondos": (re.compile(r"^rilf_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "resolucion", "timestamp", "id", "ext"]),
    "multa": (re.compile(r"^rmgen_(\d{11})_(\d{3}-\d{3}-\d{7})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "resolucion", "timestamp", "id", "ext"]),
    "notificacion": (re.compile(r"^constancia_(\d{14})_(\d{20})_(\d{13})_(\d{9})\.(pdf)$", re.IGNORECASE), ["timestamp", "resolucion", "constancia", "id", "ext"]),
    "valores": (re.compile(r"^rvalores_(\d{11})_([A-Z0-9]{12,17})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "esquela", "timestamp", "id", "ext"]),
    "ejecucion": (re.compile(r"^recgen_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "resolucion", "timestamp", "id", "ext"]),
    "baja_oficio": (re.compile(r"^bod_(\d{6})_(\d{11})_(\d{4})\.(pdf)$", re.IGNORECASE), ["codigo", "ruc", "formulario", "ext"]),
    "coactiva": (re.compile(r"^rcce_(\d{11})_ (\d{13})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["ruc", "resolucion", "timestamp", "id", "ext"]),
    "fraccionamiento": (re.compile(r"^fragen_(\d{6})_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.(pdf)$", re.IGNORECASE), ["codigo", "ruc", "resolucion", "timestamp", "id", "ext"]),
    "reporte_tributario": (re.compile(r"^reporteec_reportetrieeff_(\d{11})_(\d{14})\.(pdf)$", re.IGNORECASE), ["ruc", "timestamp", "ext"]),
    "rentas_retenciones": (re.compile(r"^reporteec_rentas_(\d{11})_(\d{14})\.(pdf)$", re.IGNORECASE), ["ruc", "timestamp"]),
    "factura_pdf": (re.compile(r"^PDF-DOC-([A-Z0-9]{4})-?(\d{1,8})(\d{11})\.(pdf)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "boleta_pdf": (re.compile(r"^PDF-BOLETA([A-Z0-9]{4})-(\d{1,8})(\d{11})\.(pdf)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "credito_pdf": (re.compile(r"^PDF-NOTA_CREDITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(pdf)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "debito_pdf": (re.compile(r"^PDF-NOTA_DEBITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(pdf)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "recibo_honorarios_pdf": (re.compile(r"^RHE(\d{11})([A-Z0-9]{4})(\d{1,8})\.(pdf)$", re.IGNORECASE), ["ruc", "serie", "correlativo", "ext"]),
    "guia_remision_pdf": (re.compile(r"^(\d{11})-09-([A-Z0-9]{4})-(\d{1,8})\.(pdf)$", re.IGNORECASE), ["ruc", "serie", "correlativo", "ext"]),
}

PATRONES_NEED_ETL = {
    # Archivos individuales que necesitan ETL
    "declaraciones_pagos": (re.compile(r"^DetalleDeclaraciones_(\d{11})_(\d{14})\.(xlsx)$", re.IGNORECASE), ["ruc", "timestamp", "ext"]),
    "guia_remision_xml": (re.compile(r"^(\d{11})-09-([A-Z0-9]{4})-(\d{1,8})\.(xml)$", re.IGNORECASE), ["ruc", "serie", "correlativo", "ext"]),

    # ZIPs estructurados que necesitan ETL
    "sire_compras": (re.compile(r"^(\d{11})-\d{8}-\d{4,6}-propuesta\.(zip|txt)$", re.IGNORECASE), ["ruc"]),
    "sire_ventas": (re.compile(r"^LE(\d{11})\d{6}1?\d+EXP2\.(zip|txt)$", re.IGNORECASE), ["ruc"]),
    "factura_xml": (re.compile(r"^FACTURA([A-Z0-9]{4})-?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "boleta_xml": (re.compile(r"^BOLETA([A-Z0-9]{4})-(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "credito_xml": (re.compile(r"^NOTA_CREDITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "debito_xml": (re.compile(r"^NOTA_DEBITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "recibo_xml": (re.compile(r"^RHE(\d{11})(\d{1,8})\.(xml)$", re.IGNORECASE), ["ruc", "correlativo", "ext"]),
    "reporte_planilla_zip": (re.compile(r"^(\d{11})_([A-Z]{3})+_(\d{8})\.(zip)$", re.IGNORECASE), ["ruc", "codigo", "fecha", "ext"]),
}

# Mapeo de columnas para SIRE Compras
COLUMN_MAPPING_COMPRAS = {
    'RUC': 'ruc',
    'Periodo': 'periodo_tributario',
    'CAR SUNAT': 'observaciones',
    'Fecha de emisión': 'fecha_emision',
    'Fecha Vcto/Pago': 'fecha_vencimiento',
    'Tipo CP/Doc.': 'tipo_comprobante',
    'Serie del CDP': 'numero_serie',
    'Nro CP o Doc. Nro Inicial (Rango)': 'numero_correlativo',
    'Tipo Doc Identidad': 'tipo_documento',
    'Nro Doc Identidad': 'numero_documento',
    'ISC': 'isc',
    'ICBPER': 'icbp',
    'Moneda': 'tipo_moneda',
    'Tipo CP Modificado': 'tipo_comprobante_modificado',
    'Serie CP Modificado': 'numero_serie_modificado',
    'Nro CP Modificado': 'numero_correlativo_modificado',
    'Detracción': 'tasa_detraccion',
}

# Mapeo de columnas para SIRE Ventas
COLUMN_MAPPING_VENTAS = {
    'Ruc': 'ruc',
    'Periodo': 'periodo_tributario',
    'CAR SUNAT': 'observaciones',
    'Fecha de emisión': 'fecha_emision',
    'Fecha Vcto/Pago': 'fecha_vencimiento',
    'Tipo CP/Doc.': 'tipo_comprobante',
    'Serie del CDP': 'numero_serie',
    'Nro CP o Doc. Nro Inicial (Rango)': 'numero_correlativo',
    'Nro Final (Rango)': 'numero_final',
    'Tipo Doc Identidad': 'tipo_documento',
    'Nro Doc Identidad': 'numero_documento',
    'ISC': 'isc',
    'ICBPER': 'icbp',
    'Moneda': 'tipo_moneda',
    'Tipo CP Modificado': 'tipo_comprobante_modificado',
    'Serie CP Modificado': 'numero_serie_modificado',
    'Nro CP Modificado': 'numero_correlativo_modificado',
}


class Config:
    # OneDrive
    ONEDRIVE_CLIENT_ID = os.getenv('ONEDRIVE_CLIENT_ID')
    ONEDRIVE_CLIENT_SECRET = os.getenv('ONEDRIVE_CLIENT_SECRET')
    ONEDRIVE_TENANT_ID = os.getenv('ONEDRIVE_TENANT_ID')
    ONEDRIVE_FOLDER_ID = os.getenv('ONEDRIVE_FOLDER_ID')

    # S3
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_key')
    AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')

    # --- PostgreSQL: ÚNICA FUENTE DE VERDAD ---
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB')

    # URL de conexión para SQLAlchemy, usada en toda la aplicación
    DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    # SQLite Queue
    QUEUE_DB_PATH = os.getenv('QUEUE_DB_PATH', 'queue.db')

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'etl_sire.log')

    # Email
    EMAIL_SMTP_SERVER = os.getenv('EMAIL_SMTP_SERVER')
    EMAIL_SMTP_PORT = int(os.getenv('EMAIL_SMTP_PORT', 587))
    EMAIL_USER = os.getenv('EMAIL_USER')
    EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
    EMAIL_TO = os.getenv('EMAIL_TO')

    # Patrones de archivos
    FILE_PATTERNS_NEED_ETL = list(PATRONES_NEED_ETL.keys())
    FILE_PATTERNS_NO_ETL = list(PATRONES_NO_ETL.keys())

    # Mapeos de columnas
    COLUMN_MAPPING_COMPRAS = COLUMN_MAPPING_COMPRAS
    COLUMN_MAPPING_VENTAS = COLUMN_MAPPING_VENTAS


# Instancia de configuración
config = Config()

def match_file_pattern(file_name):
    """
    Verifica si el archivo coincide con algún patrón estructurado y retorna el tipo, datos extraídos y si necesita ETL.
    """
    # Primero verificar NEED ETL
    for tipo, (pattern, fields) in PATRONES_NEED_ETL.items():
        match = pattern.match(file_name)
        if match:
            data = dict(zip(fields, match.groups()))
            return tipo, data, True

    # Luego NO ETL
    for tipo, (pattern, fields) in PATRONES_NO_ETL.items():
        match = pattern.match(file_name)
        if match:
            data = dict(zip(fields, match.groups()))
            return tipo, data, False

    return None, None, None

# Estrategias de verificación de procesamiento por tipo de archivo
VERIFICATION_STRATEGIES = {
    "factura": {
        "method": "single_row_check",
        "table": "facturas",
        "id_column": "numero_serie || '-' || numero_correlativo",
        "check_column": "observaciones",
        "check_value": "PROCESADO"
    },
    "boleta": {
        "method": "single_row_check",
        "table": "boletas",
        "id_column": "numero_serie || '-' || numero_correlativo",
        "check_column": "estado",
        "check_value": "COMPLETADO"
    },
    "guia_remision": {
        "method": "row_by_row_check",
        "table": "guias_remision",
        "id_columns": ["numero_guia", "item"],
        "duplicate_action": "skip"
    },
    "reporte_planilla_zip": {
        "method": "timestamp_check",
        "table": "planillas",
        "timestamp_column": "fecha_procesamiento",
        "comparison": "newer_only"
    },
    "declaraciones_pagos": {
        "method": "timestamp_check",
        "table": "declaraciones",
        "timestamp_column": "fecha_declaracion",
        "comparison": "newer_only"
    }
}

def generar_identificador_procesamiento(tipo, data):
    """
    Genera un identificador único para el archivo basado en su tipo y datos.
    """
    if tipo in ['factura', 'boleta', 'nota_credito', 'nota_debito', 'recibo_honorarios']:
        tipo_doc_map = {
            'factura': '01', 'boleta': '03', 'nota_credito': '07',
            'nota_debito': '08', 'recibo_honorarios': 'RHE'
        }
        tipo_codigo = tipo_doc_map.get(tipo, tipo)
        return f"{data.get('ruc', '')}_{tipo_codigo}_{data.get('serie', '')}_{data.get('correlativo', '')}"

    elif tipo == 'guia_remision':
        return f"{data.get('ruc', '')}_09_{data.get('serie', '')}_{data.get('correlativo', '')}"

    elif tipo == 'reporte_planilla_zip':
        return f"{data.get('ruc', '')}_PLANILLA_{data.get('periodo', '')}"

    elif tipo == 'declaraciones_pagos':
        return f"{data.get('ruc', '')}_DECLARACIONES_{data.get('timestamp', '')}"

    else:
        # Fallback: usar el nombre completo como identificador
        return data.get('file_name', '')

def extract_ruc(file_name):
    """
    Extrae el RUC del nombre del archivo basado en los patrones.
    """
    _, data, _ = match_file_pattern(file_name)
    return data.get('ruc') if data else None