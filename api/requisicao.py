#ligar as funções da main com a api no postman

from fastapi import FastAPI
from pydantic import BaseModel
import os
from datetime import datetime
from api import main  # módulo que baixa/processa CSVs
from api.upload_gcp import UploadGCP


# Caminho da chave (fora do versionamento no git)
CAMINHO_CHAVE = os.path.join("api", "chave", "sauter-university-waterworks-aa1bc765f223.json")

# Aponta para a chave do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CAMINHO_CHAVE

app = FastAPI()

# Modelo de dados esperado no POST
class IntervaloDatas(BaseModel):
    inicio: str
    fim: str

# Configurações do GCP
ID_PROJETO = "sauter-university-waterworks"
NOME_BUCKET = "datalake-ons-ena"

# Inicializa o uploader
uploader = UploadGCP(ID_PROJETO, NOME_BUCKET, CAMINHO_CHAVE)


@app.post("/coletar")
def coletar_e_enviar(intervalo: IntervaloDatas):
    pasta_local = "dados_ons"
    os.makedirs(pasta_local, exist_ok=True)

    # Baixa e processa os arquivos
    main.processar_arquivos(intervalo.inicio, intervalo.fim)

    # Lista os arquivos Parquet gerados
    arquivos_parquet = [f for f in os.listdir(pasta_local) if f.endswith(".parquet")]

    if not arquivos_parquet:
        return {"mensagem": "Nenhum arquivo gerado para o intervalo informado"}

    resultados = []
    data_ingestao = datetime.now().strftime("%Y-%m-%d")

    for arquivo in arquivos_parquet:
        caminho_local = os.path.join(pasta_local, arquivo)
        nome_blob = f"raw/reservatorios_ena/ingestion_date={data_ingestao}/{arquivo}"

        sucesso_gcs = uploader.upload_para_storage(caminho_local, nome_blob)

        resultados.append({
            "arquivo": arquivo,
            "upload_gcs": sucesso_gcs,
            "blob_path": f"gs://{NOME_BUCKET}/{nome_blob}"
        })
    return {
        "mensagem": "Processamento e uploads concluídos",
        "intervalo": {"inicio": intervalo.inicio, "fim": intervalo.fim},
        "resultados": resultados
    }
