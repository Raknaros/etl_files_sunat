#!/usr/bin/env python3
"""
Orquestador principal para ETL de documentos SUNAT.
Ejecutar con cron, e.g., 0 4 * * * /path/to/python main.py
"""

import asyncio
import logging
import sys
import os
import tempfile
from app.config import config, match_file_pattern, extract_ruc
from app.queue_db import queue_db
from app.sources.onedrive_client import onedrive_client
from app.destinations.s3_client import s3_client
from app.destinations.postgres_client import postgres_client
from app.etl_pipelines.sire_compras_etl import process_sire_compras
from app.etl_pipelines.xml_parser_etl import process_xml

# Configurar logging
logging.basicConfig(level=config.LOG_LEVEL, filename=config.LOG_FILE, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def phase_1_scan_and_classify():
    """
    Fase 1: Escaneo y Clasificación
    """
    logger.info("Iniciando Fase 1: Escaneo y Clasificación")

    # Listar archivos de OneDrive
    files = onedrive_client.list_files()

    # Filtrar archivos que coincidan con patrones estructurados
    filtered_files = []
    for f in files:
        tipo, data, needs_etl = match_file_pattern(f['name'])
        if tipo:
            f['tipo'] = tipo
            f['data'] = data
            f['needs_etl'] = needs_etl
            filtered_files.append(f)

    for file in filtered_files:
        file_name = file['name']
        file_id = file['id']  # ID del archivo en OneDrive
        download_url = file.get('@microsoft.graph.downloadUrl')  # URL de descarga
        tipo = file['tipo']

        # Determinar si necesita ETL
        needs_etl = file['needs_etl']

        if not needs_etl:
            # Archivos NO ETL: Verificar en S3
            ruc = extract_ruc(file_name)
            if ruc:
                s3_key = f"{ruc}/{file_name}"
                if s3_client.check_file_exists(s3_key):
                    logger.info(f"Duplicado S3: {file_name}")
                else:
                    # Descargar y subir a S3
                    temp_dir = tempfile.gettempdir()
                    local_path = os.path.join(temp_dir, file_name)
                    if download_url:
                        onedrive_client.download_file(download_url, local_path)
                        s3_client.upload_file(local_path, s3_key)
                        logger.info(f"Archivado directo: {file_name}")
                        # Limpiar archivo temporal
                        try:
                            os.remove(local_path)
                        except:
                            pass
                    else:
                        logger.error(f"No se pudo obtener URL de descarga para {file_name}")
            else:
                logger.warning(f"No se pudo extraer RUC de {file_name}")
        else:
            # Archivos NEED ETL: Verificar en PostgreSQL
            if postgres_client.check_file_processed(file_name):
                # Verificar S3
                if s3_client.check_file_exists(file_name):
                    logger.info(f"Duplicado S3 procesado: {file_name}")
                else:
                    # Subir a S3 si no existe
                    temp_dir = tempfile.gettempdir()
                    local_path = os.path.join(temp_dir, file_name)
                    if download_url:
                        onedrive_client.download_file(download_url, local_path)
                        s3_client.upload_file(local_path, file_name)
                        logger.info(f"Archivado procesado: {file_name}")
                        # Limpiar archivo temporal
                        try:
                            os.remove(local_path)
                        except:
                            pass
                    else:
                        logger.error(f"No se pudo obtener URL de descarga para {file_name}")
            else:
                # Insertar en queue con el ID del archivo
                queue_db.insert_task(file_name, file_id)
                logger.info(f"Tarea pendiente: {file_name}")

async def process_task(task):
    """
    Procesa una tarea individual de forma asíncrona.
    """
    task_id, file_name, file_id, _, _, _, _ = task

    try:
        # Obtener URL de descarga y descargar archivo
        download_url = onedrive_client.get_download_url(file_id)
        if not download_url:
            raise Exception(f"No se pudo obtener URL de descarga para {file_name}")

        temp_dir = tempfile.gettempdir()
        local_path = os.path.join(temp_dir, file_name)
        onedrive_client.download_file(download_url, local_path)
        
        # Determinar ETL
        if 'sire_compras' in file_name:
            data = await process_sire_compras(local_path)
        elif 'xml' in file_name:
            data = await process_xml(local_path)
        else:
            raise ValueError(f"Tipo de archivo no soportado: {file_name}")
        
        # Insertar en PostgreSQL
        postgres_client.insert_data('target_table', data)  # TODO: Tabla específica
        
        # Archivar en S3
        s3_client.upload_file(local_path, file_name)
        
        # Actualizar estado
        queue_db.update_task_status(task_id, 'PROCESADO')
        logger.info(f"Tarea procesada: {file_name}")
        
    except Exception as e:
        queue_db.update_task_status(task_id, 'ERROR', str(e))
        logger.error(f"Error procesando {file_name}: {e}")

async def phase_2_async_processing():
    """
    Fase 2: Procesamiento Asíncrono de la Cola
    """
    logger.info("Iniciando Fase 2: Procesamiento Asíncrono")
    
    tasks = queue_db.get_pending_tasks()
    if not tasks:
        logger.info("No hay tareas pendientes")
        return
    
    # Crear trabajadores asíncronos
    await asyncio.gather(*[process_task(task) for task in tasks])

def phase_3_report():
    """
    Fase 3: Reporte
    """
    logger.info("Iniciando Fase 3: Reporte")
    # TODO: Generar resumen y enviar email
    pass

async def test_no_etl_only():
    """
    Modo de prueba: Solo procesa archivos NO ETL para verificar conexiones.
    """
    logger.info("Iniciando MODO PRUEBA - Solo archivos NO ETL")

    # Crear tabla si no existe
    queue_db.create_table()

    # Modificar phase_1 para solo procesar NO ETL
    await phase_1_scan_and_classify()

    # No ejecutar fases 2 y 3 en modo prueba
    logger.info("Modo prueba completado - Solo verificación de conexiones OneDrive/S3")

async def main():
    """
    Función principal del orquestador.
    """
    # Verificar si es modo prueba
    test_mode = len(sys.argv) > 1 and sys.argv[1] == "--test"
    test_mode = test_mode or os.getenv("ETL_TEST_MODE", "false").lower() == "true"

    if test_mode:
        await test_no_etl_only()
        return

    logger.info("Iniciando ETL de documentos SUNAT")

    # Crear tabla si no existe
    queue_db.create_table()

    await phase_1_scan_and_classify()
    await phase_2_async_processing()
    phase_3_report()

    logger.info("ETL completado")

if __name__ == "__main__":
    asyncio.run(main())