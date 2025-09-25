from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import os
import main  # Seu módulo que tem a função main para baixar e processar CSVs
from upload_gcp import UploadGCP  # Supondo que a classe UploadGCP esteja neste arquivo


app = FastAPI()

# Modelo de dados esperado para o POST
class IntervaloDatas(BaseModel):
    inicio: str  # Ex.: "2025/01/01"
    fim: str     # Ex.: "2025/09/20"

# Configurações do GCP
ID_PROJETO = "sauter-university-waterworks"
NOME_BUCKET = "ons-ena-tabelas"
CAMINHO_CREDENCIAIS = "caminho/para/credenciais.json"
ID_DATASET = "nome_dataset_bigquery"

# Inicializa o uploader
uploader = UploadGCP(ID_PROJETO, NOME_BUCKET, CAMINHO_CREDENCIAIS)

@app.post("/coletar")
def coletar_e_enviar(intervalo: IntervaloDatas):
    """
    Recebe intervalo de datas, baixa os CSVs, converte para Parquet e envia para GCP.
    """
    pasta_local = "dados_ons"
    os.makedirs(pasta_local, exist_ok=True)

    # Chama a função main para baixar e processar os CSVs
    main.main(intervalo.inicio, intervalo.fim)

    # Lista todos os arquivos Parquet gerados
    arquivos_parquet = [f for f in os.listdir(pasta_local) if f.endswith(".parquet")]

    if not arquivos_parquet:
        return {"mensagem": "Nenhum arquivo gerado para o intervalo informado"}

    resultados = []

    for arquivo in arquivos_parquet:
        caminho_local = os.path.join(pasta_local, arquivo)
        nome_blob = arquivo  # Pode customizar o nome no GCS
        id_tabela = arquivo.replace(".parquet", "")  # Nome da tabela no BigQuery

        # Upload para Storage
        sucesso_gcs = uploader.upload_para_storage(caminho_local, nome_blob)

        # Upload para BigQuery
        sucesso_bq = uploader.upload_para_bigquery(caminho_local, ID_DATASET, id_tabela)

        resultados.append({
            "arquivo": arquivo,
            "upload_gcs": sucesso_gcs,
            "upload_bigquery": sucesso_bq
        })

    return {
        "mensagem": "Processamento e uploads concluídos",
        "intervalo": {"inicio": intervalo.inicio, "fim": intervalo.fim},
        "resultados": resultados
    }
