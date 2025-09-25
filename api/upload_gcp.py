from google.cloud import storage
from google.cloud import bigquery
import json
from typing import Optional
import os

class UploadGCP:
    """Classe para gerenciar uploads no Google Cloud Platform (GCP)"""
    
    def __init__(self, id_projeto: str, nome_bucket: str, caminho_credenciais: Optional[str] = None):
        self.id_projeto = id_projeto
        self.nome_bucket = nome_bucket
        
        # Configura credenciais se fornecidas
        if caminho_credenciais:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = caminho_credenciais
            
        # Cria clientes do Storage e BigQuery
        self.cliente_storage = storage.Client(project=id_projeto)
        self.cliente_bigquery = bigquery.Client(project=id_projeto)
        self.bucket = self.cliente_storage.bucket(nome_bucket)
        
    def upload_para_storage(self, caminho_local: str, nome_blob: str) -> bool:
        """Faz upload de um arquivo para o Google Cloud Storage"""
        try:
            blob = self.bucket.blob(nome_blob)
            blob.upload_from_filename(caminho_local)
            print(f"[GCS] Upload realizado: gs://{self.nome_bucket}/{nome_blob}")
            return True
        except Exception as erro:
            print(f"[ERRO GCS] Falha no upload de {caminho_local}: {erro}")
            return False
    
    def upload_para_bigquery(self, caminho_local: str, id_dataset: str, id_tabela: str) -> bool:
        """Faz upload de arquivo Parquet para o BigQuery"""
        try:
            referencia_dataset = self.cliente_bigquery.dataset(id_dataset)
            referencia_tabela = referencia_dataset.table(id_tabela)
            
            configuracao_job = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Adiciona linhas à tabela existente
                autodetect=True  # Detecta esquema automaticamente
            )
            
            # Carrega o arquivo Parquet para a tabela
            with open(caminho_local, "rb") as arquivo_fonte:
                job = self.cliente_bigquery.load_table_from_file(
                    arquivo_fonte, referencia_tabela, job_config=configuracao_job
                )
                
            job.result()  # Espera o job finalizar
            print(f"[BigQuery] Upload realizado: {self.id_projeto}.{id_dataset}.{id_tabela}")
            return True
            
        except Exception as erro:
            print(f"[ERRO BigQuery] Falha no upload de {caminho_local}: {erro}")
            return False
