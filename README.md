# ETL Documentos SUNAT

Sistema de ETL asíncrono para procesar documentos SUNAT desde OneDrive hacia PostgreSQL y S3.

## Estructura del Proyecto

```
etl_documentos_sunat/
├── main.py                 # Orquestador principal
├── requirements.txt        # Dependencias Python
├── .env                    # Variables de configuración
└── app/
    ├── config.py           # Configuración y patrones
    ├── queue_db.py         # Gestión de cola SQLite
    ├── sources/
    │   └── onedrive_client.py  # Cliente OneDrive
    └── destinations/
        ├── s3_client.py        # Cliente S3
        └── postgres_client.py  # Cliente PostgreSQL
    └── etl_pipelines/
        ├── sire_compras_etl.py
        └── xml_parser_etl.py
```

## Configuración

1. Copiar `.env` y configurar las variables:
   - Credenciales OneDrive (Client ID, Secret, Tenant)
   - Credenciales S3 (Access Key, Secret, Bucket)
   - Credenciales PostgreSQL
   - MS_REFRESH_TOKEN (opcional, para evitar device flow)

2. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```

## Modo de Prueba (Solo NO ETL)

Para probar conexiones OneDrive/S3 sin procesar archivos NEED ETL:

```bash
# Opción 1: Argumento de línea de comandos
python main.py --test

# Opción 2: Variable de entorno
ETL_TEST_MODE=true python main.py
```

Este modo:
- Lista archivos de OneDrive
- Filtra archivos NO ETL (PDFs, etc.)
- Verifica duplicados en S3
- Sube archivos nuevos con ruta `RUC/nombre_archivo`
- Genera reporte de operaciones

## Ejecución Completa

```bash
python main.py
```

Ejecuta el flujo completo:
1. **Fase 1**: Escaneo y clasificación
2. **Fase 2**: Procesamiento asíncrono de cola
3. **Fase 3**: Reporte

## Estrategias de Verificación

- **NO ETL**: Archivos directos (PDFs) - verificación en S3
- **NEED ETL**: Archivos que requieren procesamiento
  - `single_row_check`: Verificación por identificador único
  - `row_by_row_check`: Verificación fila por fila durante ETL
  - `timestamp_check`: Solo procesar versiones más recientes

## Logging

Los logs se guardan en `etl_log.log` con nivel INFO.

## Reportes

Al finalizar, genera un archivo TXT con resumen de operaciones.