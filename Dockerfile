# Use Python 3.11 slim como base
FROM python:3.11-slim

# Define o diretório de trabalho
WORKDIR /app

# Define variáveis de ambiente
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Instala dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Copia o arquivo de requirements
COPY requirements.txt .

# Instala dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código da aplicação
COPY . .

# Cria as pastas necessárias
RUN mkdir -p dados_ons logs api/chave

# Expõe a porta da aplicação
EXPOSE 8000

# Comando de saúde para verificar se o container está funcionando
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/status || exit 1

# Comando para iniciar a aplicação
CMD ["uvicorn", "api.requisicao:app", "--host", "0.0.0.0", "--port", "8000"]