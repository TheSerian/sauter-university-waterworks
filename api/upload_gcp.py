#classe pra salvar os dados do bucket pra mandar os arquivos pro gcp
from google.cloud import storage

class UploadGCP:
    def __init__(self, id_projeto, nome_bucket, caminho_chave):
        self.id_projeto = id_projeto
        self.nome_bucket = nome_bucket
        self.caminho_chave = caminho_chave

        # Cria o cliente autenticado com a chave
        self.storage_client = storage.Client.from_service_account_json(caminho_chave)
        self.bucket = self.storage_client.bucket(nome_bucket)

    def upload_para_storage(self, caminho_local, nome_blob):
        try:
            blob = self.bucket.blob(nome_blob)
            blob.upload_from_filename(caminho_local)
            return True
        except Exception as e:
            print(f"Erro ao enviar {caminho_local}: {e}")
            return False
