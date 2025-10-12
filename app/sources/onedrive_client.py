# Cliente para OneDrive - Adaptado de FilesToS3.py

import os
import msal
import requests
from app.config import config

class OneDriveClient:
    def __init__(self):
        # Configuración de Microsoft (igual que en el código de ejemplo)
        refresh_token = os.getenv('MS_REFRESH_TOKEN')
        # Solo usar refresh token si no es el placeholder y no está vacío
        use_refresh = bool(refresh_token and refresh_token.strip() and refresh_token != 'your_ms_refresh_token')
        self.ms_config = {
            "client_id": config.ONEDRIVE_CLIENT_ID or os.getenv('MS_CLIENT_ID'),
            "client_secret": config.ONEDRIVE_CLIENT_SECRET or os.getenv('MS_CLIENT_SECRET'),
            "refresh_token": refresh_token,
            "authority": "https://login.microsoftonline.com/common",
            "scopes": ['Files.ReadWrite.All'],
            "use_refresh": use_refresh  # True solo si hay refresh token válido
        }
        self.token = None

    def _get_token(self):
        """Obtiene access token de Microsoft Graph usando Public Client (compatible con refresh tokens de device flow)."""
        config_ms = self.ms_config

        # Siempre usar PublicClientApplication (compatible con refresh tokens de device flow)
        app = msal.PublicClientApplication(
            client_id=config_ms["client_id"],
            authority=config_ms["authority"]
        )

        if config_ms.get("use_refresh"):
            # Usar refresh token con Public Client
            result = app.acquire_token_by_refresh_token(config_ms["refresh_token"], scopes=config_ms["scopes"])
        else:
            # Usar device flow
            flow = app.initiate_device_flow(scopes=config_ms["scopes"])
            if "user_code" not in flow:
                raise ValueError("Fallo al crear el flujo de dispositivo.", flow.get("error_description"))

            print(flow["message"])
            result = app.acquire_token_by_device_flow(flow)
            # Imprimir refresh token para configuración
            if "access_token" in result and "refresh_token" in result:
                print(f"\n--- REFRESH TOKEN PARA .ENV ---\nMS_REFRESH_TOKEN={result['refresh_token']}\n-------------------------------\n")

        if "access_token" in result:
            self.token = result["access_token"]
            return self.token
        else:
            raise Exception(f"No se pudo obtener el access token: {result.get('error_description')}")

    def list_files(self, folder_path="AbacoBot"):
        """
        Lista RECURSIVAMENTE todos los archivos dentro de una carpeta específica
        en OneDrive usando el endpoint de children.
        Retorna lista de dicts con info de archivos.
        """
        # Obtener token primero
        token = self._get_token()

        # Usar la lógica del código de ejemplo
        headers = {'Authorization': 'Bearer ' + token}
        archivos_totales = []

        def recurse(current_path):
            endpoint = f"https://graph.microsoft.com/v1.0/me/drive/root:/{current_path}:/children"
            try:
                response = requests.get(endpoint, headers=headers)
                response.raise_for_status()
                data = response.json()

                for item in data.get('value', []):
                    if 'folder' in item:
                        # Es una carpeta, recursar
                        sub_path = f"{current_path}/{item['name']}"
                        recurse(sub_path)
                    else:
                        # Es un archivo, añadir a la lista
                        archivos_totales.append(item)

            except requests.exceptions.RequestException as e:
                print(f"❌ Error al listar {current_path}: {e}")
                if hasattr(e, 'response') and e.response:
                    print(f"Detalles: {e.response.json()}")

        print(f"Buscando todos los archivos dentro de la carpeta '{folder_path}' en OneDrive...")
        recurse(folder_path)
        return archivos_totales

    def get_download_url(self, file_id):
        """
        Obtiene la URL de descarga de un archivo usando su ID.
        """
        token = self._get_token()
        headers = {'Authorization': 'Bearer ' + token}
        endpoint = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}"

        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data.get('@microsoft.graph.downloadUrl')
        except requests.exceptions.RequestException as e:
            print(f"❌ Error al obtener download URL para {file_id}: {e}")
            return None

    def download_file(self, download_url, local_path):
        """
        Descarga un archivo de OneDrive usando la download URL.
        """
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    def delete_file(self, file_id):
        """
        Elimina un archivo de OneDrive usando su ID.
        """
        token = self._get_token()
        headers = {'Authorization': 'Bearer ' + token}
        endpoint = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}"

        try:
            response = requests.delete(endpoint, headers=headers)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"❌ Error al eliminar archivo {file_id}: {e}")
            return False

# Instancia
onedrive_client = OneDriveClient()