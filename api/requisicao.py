#ligar as funções da main com a api no postman

from fastapi import FastAPI, Query
from pydantic import BaseModel
import os
from datetime import datetime
from api import main  # módulo que baixa/processa CSVs
from api.upload_gcp import UploadGCP
import logging
from typing import Optional, List

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/api.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Criar pasta de logs se não existir
os.makedirs('logs', exist_ok=True)

# Caminho da chave (fora do versionamento no git)
CAMINHO_CHAVE = os.path.join("api", "chave", "sauter-university-waterworks-aa1bc765f223.json")

# Aponta para a chave do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CAMINHO_CHAVE

app = FastAPI(
    title="ONS ENA Data Collector",
    description="API para coleta de dados ENA do ONS e envio para GCP",
    version="1.0.0"
)

# Modelo de dados esperado no POST
class IntervaloDatas(BaseModel):
    inicio: str
    fim: str

# Configurações do GCP
ID_PROJETO = "sauter-university-waterworks"
NOME_BUCKET = "datalake-ons-ena"

# Inicializa o uploader
uploader = UploadGCP(ID_PROJETO, NOME_BUCKET, CAMINHO_CHAVE)

@app.get("/")
def root():
    """Endpoint básico de teste"""
    logger.info("Endpoint raiz acessado")
    return {
        "message": "ONS ENA Data Collector API está rodando!",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/coletar")
def coletar_e_enviar(intervalo: IntervaloDatas):
    """Coleta dados do ONS e envia para GCP"""
    logger.info(f"Nova solicitação de coleta recebida - Início: {intervalo.inicio}, Fim: {intervalo.fim}")
    
    pasta_local = "dados_ons"
    os.makedirs(pasta_local, exist_ok=True)

    try:
        # Baixa e processa os arquivos
        logger.info("Iniciando processamento dos arquivos")
        main.processar_arquivos(intervalo.inicio, intervalo.fim)

        # Lista os arquivos Parquet gerados
        arquivos_parquet = [f for f in os.listdir(pasta_local) if f.endswith(".parquet")]
        logger.info(f"Encontrados {len(arquivos_parquet)} arquivos Parquet para upload")

        if not arquivos_parquet:
            logger.warning("Nenhum arquivo gerado para o intervalo informado")
            return {"mensagem": "Nenhum arquivo gerado para o intervalo informado"}

        resultados = []
        data_ingestao = datetime.now().strftime("%Y-%m-%d")

        for arquivo in arquivos_parquet:
            caminho_local = os.path.join(pasta_local, arquivo)
            nome_blob = f"raw/reservatorios_ena/ingestion_date={data_ingestao}/{arquivo}"

            logger.info(f"Iniciando upload do arquivo: {arquivo}")
            sucesso_gcs = uploader.upload_para_storage(caminho_local, nome_blob)
            
            if sucesso_gcs:
                logger.info(f"Upload realizado com sucesso: {arquivo}")
            else:
                logger.error(f"Falha no upload: {arquivo}")

            resultados.append({
                "arquivo": arquivo,
                "upload_gcs": sucesso_gcs,
                "blob_path": f"gs://{NOME_BUCKET}/{nome_blob}"
            })

        sucessos = sum(1 for r in resultados if r["upload_gcs"])
        logger.info(f"Processamento concluído - {sucessos}/{len(arquivos_parquet)} uploads realizados com sucesso")

        return {
            "mensagem": "Processamento e uploads concluídos",
            "intervalo": {"inicio": intervalo.inicio, "fim": intervalo.fim},
            "resultados": resultados,
            "resumo": {
                "total_arquivos": len(arquivos_parquet),
                "uploads_sucesso": sucessos,
                "uploads_falha": len(arquivos_parquet) - sucessos
            }
        }
        
    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        return {
            "mensagem": "Erro durante o processamento",
            "erro": str(e)
        }

@app.get("/arquivos")
def listar_arquivos(
    page: int = Query(1, ge=1, description="Número da página"),
    size: int = Query(10, ge=1, le=100, description="Itens por página")
):
    """Lista arquivos na pasta local com paginação"""
    logger.info(f"Listagem de arquivos solicitada - Página: {page}, Tamanho: {size}")
    
    pasta_local = "dados_ons"
    
    if not os.path.exists(pasta_local):
        logger.warning("Pasta de dados não encontrada")
        return {
            "items": [],
            "total": 0,
            "page": page,
            "size": size,
            "has_next": False
        }
    
    # Lista todos os arquivos
    arquivos = []
    for arquivo in os.listdir(pasta_local):
        if arquivo.endswith(('.parquet', '.csv')):
            caminho_completo = os.path.join(pasta_local, arquivo)
            stat = os.stat(caminho_completo)
            
            arquivos.append({
                "nome": arquivo,
                "tamanho_mb": round(stat.st_size / 1024 / 1024, 2),
                "modificado": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "extensao": arquivo.split('.')[-1]
            })
    
    # Ordena por data de modificação (mais recente primeiro)
    arquivos.sort(key=lambda x: x['modificado'], reverse=True)
    
    # Paginação
    total = len(arquivos)
    start_idx = (page - 1) * size
    end_idx = start_idx + size
    page_items = arquivos[start_idx:end_idx]
    
    logger.info(f"Retornando {len(page_items)} arquivos de {total} total")
    
    return {
        "items": page_items,
        "total": total,
        "page": page,
        "size": size,
        "has_next": end_idx < total
    }

@app.get("/status")
def status_sistema():
    """Verifica status do sistema"""
    logger.info("Verificação de status solicitada")
    
    pasta_dados = "dados_ons"
    pasta_logs = "logs"
    chave_gcp = CAMINHO_CHAVE
    
    status = {
        "timestamp": datetime.now().isoformat(),
        "sistema": "ONS ENA Data Collector",
        "versao": "1.0.0",
        "status": "online",
        "verificacoes": {
            "pasta_dados_existe": os.path.exists(pasta_dados),
            "pasta_logs_existe": os.path.exists(pasta_logs),
            "chave_gcp_existe": os.path.exists(chave_gcp)
        }
    }
    
    # Conta arquivos se a pasta existir
    if os.path.exists(pasta_dados):
        arquivos = [f for f in os.listdir(pasta_dados) if f.endswith(('.parquet', '.csv'))]
        status["arquivos_locais"] = len(arquivos)
    
    logger.info(f"Status do sistema: {status['verificacoes']}")
    return status

if __name__ == "__main__":
    import uvicorn
    logger.info("Iniciando servidor da API")
    uvicorn.run(app, host="0.0.0.0", port=8000)