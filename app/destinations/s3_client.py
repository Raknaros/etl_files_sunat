# Cliente para S3 - Adaptado de FilesToS3.py

import boto3
import requests
from app.config import config

class S3Client:
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
        )
        self.bucket = config.AWS_S3_BUCKET_NAME

    def check_file_exists(self, key):
        """
        Verifica si un objeto existe en S3 usando list_objects_v2.
        Retorna True si existe, False si no.
        """
        try:
            # Pide una lista de objetos que coincidan exactamente con la clave (key)
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=1)

            # Si 'Contents' existe en la respuesta y el nombre de la clave es exacto,
            # significa que el archivo fue encontrado.
            if 'Contents' in response and response['Contents'][0]['Key'] == key:
                return True
            return False
        except self.s3.exceptions.NoSuchBucket:
            print(f"⚠️  Bucket S3 '{self.bucket}' no existe. Omitiendo verificación S3.")
            return False
        except self.s3.exceptions.ClientError as e:
            # Manejar otros posibles errores, como acceso denegado a ListBucket
            print(f"Error de cliente de AWS al verificar el archivo: {e}")
            return False

    def upload_file(self, local_path, key):
        """
        Sube un archivo local a S3 con la clave key.
        """
        try:
            self.s3.upload_file(local_path, self.bucket, key)
            print(f"Archivo subido a S3: {key}")
        except self.s3.exceptions.NoSuchBucket:
            print(f"⚠️  Bucket S3 '{self.bucket}' no existe. Omitiendo subida a S3.")
        except Exception as e:
            print(f"Error al subir archivo a S3: {e}")
            raise

    def upload_from_url(self, url, key):
        """
        Descarga de URL y sube a S3 en streaming.
        """
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            self.s3.upload_fileobj(r.raw, self.bucket, key)

# Instancia
s3_client = S3Client()