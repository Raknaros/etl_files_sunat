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
import zipfile
import rarfile
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

def process_compressed_file(file_name, file_id, download_url):
    """
    Procesa un archivo comprimido (.zip, .rar) y extrae archivos que necesiten ETL.
    Ahora también detecta ZIPs estructurados dentro de contenedores.
    Retorna lista de archivos internos que necesitan procesamiento.
    """
    temp_dir = tempfile.gettempdir()
    compressed_path = os.path.join(temp_dir, file_name)

    try:
        # Descargar archivo comprimido
        onedrive_client.download_file(download_url, compressed_path)

        extracted_files = []

        # Determinar tipo de archivo comprimido
        if file_name.lower().endswith('.zip'):
            with zipfile.ZipFile(compressed_path, 'r') as zf:
                # Listar archivos internos
                for info in zf.filelist:
                    internal_name = info.filename
                    tipo, data, needs_etl = match_file_pattern(internal_name)

                    if tipo:  # Archivo interno coincide con patrón
                        extracted_files.append({
                            'internal_name': internal_name,
                            'tipo': tipo,
                            'data': data,
                            'needs_etl': needs_etl,
                            'compressed_path': compressed_path,  # Mantener el archivo comprimido para extracción posterior
                            'file_info': info,
                            'is_from_container': True  # Marcar que viene de contenedor
                        })

        elif file_name.lower().endswith('.rar'):
            try:
                with rarfile.RarFile(compressed_path, 'r') as rf:
                    # Listar archivos internos
                    for info in rf.infolist():
                        internal_name = info.filename
                        tipo, data, needs_etl = match_file_pattern(internal_name)

                        if tipo:  # Archivo interno coincide con patrón
                            extracted_files.append({
                                'internal_name': internal_name,
                                'tipo': tipo,
                                'data': data,
                                'needs_etl': needs_etl,
                                'compressed_path': compressed_path,  # Mantener el archivo comprimido para extracción posterior
                                'file_info': info,
                                'is_from_container': True  # Marcar que viene de contenedor
                            })
            except NameError:
                logger.warning(f"rarfile no instalado, omitiendo {file_name}")

        # NO limpiar el archivo comprimido aquí - se necesita para extracción posterior
        # El archivo se limpiará después de procesar todos los archivos internos

        return extracted_files

    except Exception as e:
        logger.error(f"Error procesando archivo comprimido {file_name}: {e}")
        # Limpiar archivo comprimido en caso de error
        try:
            os.remove(compressed_path)
        except:
            pass
        return []


async def phase_1_scan_and_classify():
    """
    Fase 1: Escaneo y Clasificación
    """
    logger.info("Iniciando Fase 1: Escaneo y Clasificación")

    # Listar archivos de OneDrive
    files = onedrive_client.list_files()

    # Filtrar archivos que coincidan con patrones estructurados
    filtered_files = []
    compressed_files = []

    for f in files:
        file_name = f['name']

        # Verificar si es archivo comprimido
        if file_name.lower().endswith(('.zip', '.rar')):
            # Verificar si es ZIP estructurado (NEED ETL) o contenedor genérico
            tipo, data, needs_etl = match_file_pattern(file_name)
            if tipo and needs_etl:
                # ZIP estructurado - tratar como archivo individual NEED ETL
                f['tipo'] = tipo
                f['data'] = data
                f['needs_etl'] = needs_etl
                f['is_structured_zip'] = True  # Marcar como ZIP estructurado
                filtered_files.append(f)
            else:
                # ZIP contenedor - agregar para inspección
                compressed_files.append(f)
        else:
            # Verificar si coincide con patrones normales
            tipo, data, needs_etl = match_file_pattern(file_name)
            if tipo:
                f['tipo'] = tipo
                f['data'] = data
                f['needs_etl'] = needs_etl
                filtered_files.append(f)

    # Procesar archivos comprimidos
    compressed_to_cleanup = []  # Lista de archivos comprimidos para limpiar después

    for compressed_file in compressed_files:
        file_name = compressed_file['name']
        file_id = compressed_file['id']
        download_url = compressed_file.get('@microsoft.graph.downloadUrl')

        if download_url:
            logger.info(f"Procesando archivo comprimido: {file_name}")
            internal_files = process_compressed_file(file_name, file_id, download_url)

            # Agregar archivos internos que coincidan con patrones
            for internal_file in internal_files:
                # Crear entrada para archivo interno
                internal_entry = {
                    'name': internal_file['internal_name'],
                    'tipo': internal_file['tipo'],
                    'data': internal_file['data'],
                    'needs_etl': internal_file['needs_etl'],
                    'parent_compressed': file_name,
                    'compressed_path': internal_file['compressed_path'],
                    'file_info': internal_file['file_info'],
                    'is_from_container': internal_file.get('is_from_container', False)
                }
                filtered_files.append(internal_entry)

            # Agregar a lista de cleanup si hay archivos internos
            if internal_files:
                compressed_to_cleanup.append(internal_file['compressed_path'])

    # Procesar todos los archivos filtrados (normales + internos de comprimidos)
    for file in filtered_files:
        file_name = file['name']
        tipo = file['tipo']
        needs_etl = file['needs_etl']

        # Determinar si viene de archivo comprimido
        is_from_compressed = 'parent_compressed' in file
        is_structured_zip = file.get('is_structured_zip', False)
        is_from_container = file.get('is_from_container', False)

        if not needs_etl:
            # Archivos NO ETL
            ruc = extract_ruc(file_name)
            if ruc:
                s3_key = f"{ruc}/{file_name}"
                if s3_client.check_file_exists(s3_key):
                    logger.info(f"Duplicado S3: {file_name}")
                    # OPCIONAL: Eliminar archivo duplicado de OneDrive
                    delete_duplicates = os.getenv('DELETE_DUPLICATES_ONEDRIVE', 'false').lower() == 'true'
                    if delete_duplicates:
                        file_id = file.get('id')
                        if file_id and onedrive_client.delete_file(file_id):
                            logger.info(f"Eliminado de OneDrive (duplicado): {file_name}")
                else:
                    if is_from_compressed:
                        # Extraer del archivo comprimido y subir
                        await extract_and_upload_from_compressed(file, s3_key)
                    else:
                        # Archivo normal: descargar y subir
                        file_id = file.get('id')
                        download_url = file.get('@microsoft.graph.downloadUrl')
                        if download_url:
                            temp_dir = tempfile.gettempdir()
                            local_path = os.path.join(temp_dir, file_name)
                            onedrive_client.download_file(download_url, local_path)
                            s3_client.upload_file(local_path, s3_key)
                            logger.info(f"Archivado directo: {file_name}")
                            try:
                                os.remove(local_path)
                            except:
                                pass
            else:
                logger.warning(f"No se pudo extraer RUC de {file_name}")
        else:
            # Archivos NEED ETL: Verificar en PostgreSQL (solo si no es modo prueba)
            test_mode = len(sys.argv) > 1 and sys.argv[1] == "--test"
            test_mode = test_mode or os.getenv("ETL_TEST_MODE", "false").lower() == "true"

            if not test_mode:
                if postgres_client.check_file_processed(file_name):
                    # Verificar S3
                    if s3_client.check_file_exists(file_name):
                        logger.info(f"Duplicado S3 procesado: {file_name}")
                    else:
                        await process_structured_zip_file(file, tipo)
                else:
                    # TODOS los NEED ETL van a procesamiento asíncrono
                    if is_from_container:
                        # ZIP estructurado dentro de contenedor
                        queue_db.insert_task(file_name, f"container:{file['parent_compressed']}:{file_name}")
                    elif is_structured_zip:
                        # ZIP estructurado individual
                        file_id = file.get('id')
                        queue_db.insert_task(file_name, file_id)
                    elif is_from_compressed:
                        # Archivo de comprimido
                        queue_db.insert_task(file_name, f"compressed:{file['parent_compressed']}:{file_name}")
                    else:
                        # Archivo individual
                        file_id = file.get('id')
                        queue_db.insert_task(file_name, file_id)
                    logger.info(f"Tarea pendiente: {file_name}")
            else:
                # En modo prueba, registrar como tarea pendiente sin verificar PostgreSQL
                if is_from_container:
                    queue_db.insert_task(file_name, f"container:{file['parent_compressed']}:{file_name}")
                elif is_structured_zip:
                    file_id = file.get('id')
                    queue_db.insert_task(file_name, file_id)
                elif is_from_compressed:
                    queue_db.insert_task(file_name, f"compressed:{file['parent_compressed']}:{file_name}")
                else:
                    file_id = file.get('id')
                    queue_db.insert_task(file_name, file_id)
                logger.info(f"Tarea pendiente (modo prueba): {file_name}")

    # Limpiar archivos comprimidos después de procesar todos los archivos internos
    for compressed_path in set(compressed_to_cleanup):
        try:
            os.remove(compressed_path)
            logger.info(f"Archivo comprimido limpiado: {os.path.basename(compressed_path)}")
        except Exception as e:
            logger.warning(f"No se pudo limpiar archivo comprimido {compressed_path}: {e}")


async def process_structured_zip_file(file, tipo):
    """
    Procesa un archivo ZIP estructurado (individual o de contenedor).
    """
    file_name = file['name']
    file_id = file.get('id')
    download_url = file.get('@microsoft.graph.downloadUrl')
    is_from_container = file.get('is_from_container', False)

    if is_from_container:
        # ZIP estructurado dentro de contenedor
        parent_compressed = file['parent_compressed']
        compressed_path = file['compressed_path']

        # Extraer del contenedor
        temp_dir = tempfile.gettempdir()
        local_path = os.path.join(temp_dir, file_name)

        if compressed_path.endswith('.zip'):
            with zipfile.ZipFile(compressed_path, 'r') as zf:
                zf.extract(file_name, temp_dir)
        elif compressed_path.endswith('.rar'):
            try:
                with rarfile.RarFile(compressed_path, 'r') as rf:
                    rf.extract(file_name, temp_dir)
            except NameError:
                logger.error("rarfile no instalado")
                return
    else:
        # ZIP estructurado individual
        if not download_url:
            logger.error(f"No hay URL de descarga para {file_name}")
            return

        temp_dir = tempfile.gettempdir()
        local_path = os.path.join(temp_dir, file_name)
        onedrive_client.download_file(download_url, local_path)

    try:
        # Procesar con pipeline correspondiente
        if 'reporte_planilla_zip' in tipo:
            data = await process_sire_compras(local_path)
        elif 'xml' in tipo:
            data = await process_xml(local_path)
        else:
            raise ValueError(f"Tipo de ZIP estructurado no soportado: {tipo}")

        # Insertar en PostgreSQL
        postgres_client.insert_data('target_table', data)

        # Subir a S3
        s3_client.upload_file(local_path, file_name)
        logger.info(f"ZIP estructurado procesado: {file_name}")

    except Exception as e:
        logger.error(f"Error procesando ZIP estructurado {file_name}: {e}")
    finally:
        # Limpiar archivo local
        try:
            os.remove(local_path)
        except:
            pass

async def extract_and_upload_from_compressed(file_info, s3_key):
    """
    Extrae un archivo de un comprimido y lo sube a S3.
    """
    internal_name = file_info.get('internal_name', file_info.get('name', 'unknown'))
    compressed_path = file_info.get('compressed_path')
    parent_compressed = file_info.get('parent_compressed', 'unknown')

    try:
        if not compressed_path or not os.path.exists(compressed_path):
            raise Exception(f"Archivo comprimido no encontrado: {compressed_path}")

        temp_dir = tempfile.gettempdir()
        extracted_path = os.path.join(temp_dir, os.path.basename(internal_name))

        # Extraer archivo del comprimido
        if compressed_path.endswith('.zip'):
            with zipfile.ZipFile(compressed_path, 'r') as zf:
                zf.extract(internal_name, temp_dir)
        elif compressed_path.endswith('.rar'):
            try:
                with rarfile.RarFile(compressed_path, 'r') as rf:
                    rf.extract(internal_name, temp_dir)
            except NameError:
                logger.error("rarfile no instalado")
                return

        # Verificar que el archivo fue extraído
        if not os.path.exists(extracted_path):
            raise Exception(f"No se pudo extraer {internal_name}")

        # Subir a S3
        s3_client.upload_file(extracted_path, s3_key)
        logger.info(f"Extraído y archivado: {internal_name} de {parent_compressed}")

        # Limpiar archivo extraído
        try:
            os.remove(extracted_path)
        except:
            pass

    except Exception as e:
        logger.error(f"Error extrayendo {internal_name} de {parent_compressed}: {e}")

async def process_task(task):
    """
    Procesa una tarea individual de forma asíncrona.
    """
    task_id, file_name, file_id, _, _, _, _ = task

    try:
        temp_dir = tempfile.gettempdir()
        local_path = os.path.join(temp_dir, file_name)

        # Verificar si viene de archivo comprimido
        if file_id.startswith("compressed:"):
            # Formato: compressed:nombre_comprimido:nombre_interno
            _, compressed_name, internal_name = file_id.split(":", 2)

            # Encontrar el archivo comprimido en OneDrive
            files = onedrive_client.list_files()
            compressed_file = next((f for f in files if f['name'] == compressed_name), None)

            if not compressed_file:
                raise Exception(f"No se encontró archivo comprimido {compressed_name}")

            # Descargar y extraer
            compressed_id = compressed_file['id']
            download_url = onedrive_client.get_download_url(compressed_id)
            if not download_url:
                raise Exception(f"No se pudo obtener URL de descarga para {compressed_name}")

            compressed_path = os.path.join(temp_dir, compressed_name)
            onedrive_client.download_file(download_url, compressed_path)

            # Extraer archivo específico
            if compressed_path.endswith('.zip'):
                with zipfile.ZipFile(compressed_path, 'r') as zf:
                    zf.extract(internal_name, temp_dir)
                    # Mover a local_path
                    extracted_path = os.path.join(temp_dir, internal_name)
                    if os.path.exists(extracted_path):
                        os.rename(extracted_path, local_path)
            elif compressed_path.endswith('.rar'):
                try:
                    with rarfile.RarFile(compressed_path, 'r') as rf:
                        rf.extract(internal_name, temp_dir)
                        extracted_path = os.path.join(temp_dir, internal_name)
                        if os.path.exists(extracted_path):
                            os.rename(extracted_path, local_path)
                except NameError:
                    raise Exception("rarfile no instalado para archivos RAR")

            # Limpiar archivo comprimido
            try:
                os.remove(compressed_path)
            except:
                pass

        elif file_id.startswith("container:"):
            # Formato: container:nombre_contenedor:nombre_zip_estructurado
            _, container_name, structured_zip_name = file_id.split(":", 2)

            # Encontrar el contenedor en OneDrive
            files = onedrive_client.list_files()
            container_file = next((f for f in files if f['name'] == container_name), None)

            if not container_file:
                raise Exception(f"No se encontró contenedor {container_name}")

            # Descargar contenedor
            container_id = container_file['id']
            download_url = onedrive_client.get_download_url(container_id)
            if not download_url:
                raise Exception(f"No se pudo obtener URL de descarga para {container_name}")

            container_path = os.path.join(temp_dir, container_name)
            onedrive_client.download_file(download_url, container_path)

            # Extraer ZIP estructurado del contenedor
            if container_path.endswith('.zip'):
                with zipfile.ZipFile(container_path, 'r') as zf:
                    zf.extract(structured_zip_name, temp_dir)
                    # Mover a local_path
                    extracted_path = os.path.join(temp_dir, structured_zip_name)
                    if os.path.exists(extracted_path):
                        os.rename(extracted_path, local_path)
            elif container_path.endswith('.rar'):
                try:
                    with rarfile.RarFile(container_path, 'r') as rf:
                        rf.extract(structured_zip_name, temp_dir)
                        extracted_path = os.path.join(temp_dir, structured_zip_name)
                        if os.path.exists(extracted_path):
                            os.rename(extracted_path, local_path)
                except NameError:
                    raise Exception("rarfile no instalado para archivos RAR")

            # Limpiar contenedor
            try:
                os.remove(container_path)
            except:
                pass

        else:
            # Archivo normal: obtener URL de descarga y descargar
            download_url = onedrive_client.get_download_url(file_id)
            if not download_url:
                raise Exception(f"No se pudo obtener URL de descarga para {file_name}")

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