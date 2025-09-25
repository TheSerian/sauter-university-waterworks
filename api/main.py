#criação das funçoes pra baixar os arquivos, filtrar e mandar pra api
import requests
import pandas as pd
import os
import re
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq


def baixar_por_resource_id(resource_id, nome_arquivo):
    """
    Baixa arquivo do ONS usando resource_id
    """
    try:
        resp = requests.get(f"https://dados.ons.org.br/api/3/action/resource_show?id={resource_id}")
        resp.raise_for_status()
        url_real = resp.json()["result"]["url"]

        r = requests.get(url_real, timeout=60)
        r.raise_for_status()

        with open(nome_arquivo, "wb") as f:
            f.write(r.content)

        print(f"[OK] Baixado: {nome_arquivo}")
        return True

    except Exception as e:
        print(f"[ERRO] Falha ao baixar {nome_arquivo} pelo resource_id {resource_id}: {e}")
        return False


def limpar_nome_colunas(nome_coluna: str) -> str:
    """
    Limpa nomes de colunas para que sejam compatíveis com BigQuery.
    Remove espaços, caracteres especiais e substitui por underscore.
    """
    nome_coluna = re.sub(r'[^0-9a-zA-Z_]', '_', nome_coluna)
    nome_coluna = re.sub(r'_+', '_', nome_coluna)  # substitui múltiplos underscores
    nome_coluna = nome_coluna.strip('_')  # remove underscores no início/fim
    return nome_coluna


def df_para_parquet_em_string(df: pd.DataFrame, arquivo_parquet: str):
    """
    Converte um DataFrame para Parquet garantindo que todas as colunas sejam STRING.
    """
    # Limpa nomes de colunas
    df.columns = [limpar_nome_colunas(coluna) for coluna in df.columns]

    # Garante que todos os campos sejam string
    for coluna in df.columns:
        df[coluna] = df[coluna].astype(str)

    # Cria esquema PyArrow com todas colunas STRING
    esquema = pa.schema([(coluna, pa.string()) for coluna in df.columns])
    tabela = pa.Table.from_pandas(df, schema=esquema, preserve_index=False)

    # Salva Parquet
    pq.write_table(tabela, arquivo_parquet, compression="snappy")
    print(f"[PARQUET] Arquivo criado: {arquivo_parquet}")


def processar_arquivos(data_inicio: str, data_fim: str):
    pasta = "dados_ons"
    os.makedirs(pasta, exist_ok=True)
    print(f"[PASTA] Pasta criada ou já existente: {pasta}")

    inicio = datetime.strptime(data_inicio, "%Y/%m/%d")
    fim = datetime.strptime(data_fim, "%Y/%m/%d")

    id_dataset = "ena-diario-por-bacia"
    url_api = f"https://dados.ons.org.br/api/3/action/package_show?id={id_dataset}"

    try:
        resp = requests.get(url_api)
        resp.raise_for_status()
        dados = resp.json()
        recursos = dados["result"]["resources"]
        print(f"[INFO] Encontrados {len(recursos)} recursos na API")
    except Exception as erro:
        print(f"[ERRO] Erro ao acessar API do ONS: {erro}")
        return

    anos = list(range(inicio.year, fim.year + 1))
    sucessos = 0

    for ano in anos:
        print(f"\n[PROCESSANDO] Ano: {ano}")
        encontrou_ano = False

        for r in recursos:
            nome_recurso = r.get("name", "")
            resource_id = r.get("id")

            # Busca ano no nome do recurso (ex: ENA 2024)
            match = re.search(r"\b(20\d{2})\b", nome_recurso)
            if not match:
                continue
            ano_recurso = int(match.group(1))

            if ano != ano_recurso:
                continue

            encontrou_ano = True
            arquivo_csv = os.path.join(pasta, f"ENA_{ano}_original.csv")
            arquivo_parquet = os.path.join(pasta, f"ENA_{ano}.parquet")

            # Download
            if not baixar_por_resource_id(resource_id, arquivo_csv):
                continue

            try:
                print(f"[CONVERSAO] Processando {arquivo_csv}...")

                # Lê CSV (tentando UTF-8 e depois Latin1)
                try:
                    df = pd.read_csv(
                        arquivo_csv,
                        sep=";",
                        encoding="utf-8",
                        engine="python",
                        on_bad_lines="skip"
                    )
                except UnicodeDecodeError:
                    df = pd.read_csv(
                        arquivo_csv,
                        sep=";",
                        encoding="latin1",
                        engine="python",
                        on_bad_lines="skip"
                    )

                print(f"[INFO] Arquivo carregado: {len(df)} linhas, {len(df.columns)} colunas")
                print(f"[DEBUG] Colunas disponíveis: {list(df.columns)}")

                # Converte coluna de data
                if "ena_data" in df.columns:
                    df["ena_data"] = pd.to_datetime(df["ena_data"], errors="coerce", dayfirst=True)
                else:
                    print(f"[ERRO] Coluna 'ena_data' não encontrada em {nome_recurso}")
                    continue

                # Filtra intervalo de datas
                df_filtrado = df[(df["ena_data"] >= inicio) & (df["ena_data"] <= fim)].copy()
                print(f"[INFO] Após filtro de data: {len(df_filtrado)} linhas")

                if len(df_filtrado) > 0:
                    df_para_parquet_em_string(df_filtrado, arquivo_parquet)

                    tamanho_csv = os.path.getsize(arquivo_csv) / 1024 / 1024
                    tamanho_parquet = os.path.getsize(arquivo_parquet) / 1024 / 1024
                    reducao = ((tamanho_csv - tamanho_parquet) / tamanho_csv) * 100

                    print(f"[SUCESSO] Arquivo salvo: {arquivo_parquet}")
                    print(f"  CSV: {tamanho_csv:.2f} MB | Parquet: {tamanho_parquet:.2f} MB | Redução: {reducao:.1f}%")
                    sucessos += 1
                else:
                    print(f"[AVISO] Nenhum dado no intervalo para {ano}")

            except Exception as erro:
                print(f"[ERRO] Falha ao processar {arquivo_csv}: {erro}")

            break  # já encontrou o recurso do ano, não precisa continuar

        if not encontrou_ano:
            print(f"[AVISO] Não encontrado recurso para o ano {ano}")

    # Relatório final
    print(f"\n{'='*50}")
    print(f"[RELATÓRIO FINAL]")
    print(f"Anos processados: {len(anos)}")
    print(f"Sucessos: {sucessos}")
    print(f"Pasta: {pasta}")

    arquivos_parquet = sorted([f for f in os.listdir(pasta) if f.endswith('.parquet')])
    if arquivos_parquet:
        print(f"\n[ARQUIVOS GERADOS]")
        total_size = 0
        for arq in arquivos_parquet:
            caminho = os.path.join(pasta, arq)
            tamanho = os.path.getsize(caminho) / 1024 / 1024
            total_size += tamanho
            print(f"  {arq} ({tamanho:.2f} MB)")
        print(f"\nTamanho total: {total_size:.2f} MB")

    print(f"\n[CONCLUÍDO] - {sucessos}/{len(anos)} anos processados com sucesso")


if __name__ == "__main__":
    processar_arquivos(data_inicio="2025/01/01", data_fim="2025/09/20")
