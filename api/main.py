import requests
import pandas as pd
import os
import re
from datetime import datetime


def baixar_por_resource_id(resource_id, nome_arquivo):
    """
    Baixa arquivo do ONS usando resource_id
    """
    try:
        # URL do recurso
        resp = requests.get(f"https://dados.ons.org.br/api/3/action/resource_show?id={resource_id}")
        resp.raise_for_status()
        url_real = resp.json()["result"]["url"]

        # Faz download do CSV
        r = requests.get(url_real, timeout=60)
        r.raise_for_status()

        with open(nome_arquivo, "wb") as f:
            f.write(r.content)

        print(f"[OK] Baixado: {nome_arquivo}")
        return True

    except Exception as e:
        print(f"[ERRO] Falha ao baixar {nome_arquivo} pelo resource_id {resource_id}: {e}")
        return False

def main(data_inicio: str, data_fim: str):
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

    anos = list(range(inicio.year, fim.year+1))
    sucessos = 0

    for ano in anos:
        print(f"\n[PROCESSANDO] Ano: {ano}")
        encontrou_ano = False

        for r in recursos:
            nome_recurso = r.get("name", "")
            resource_id = r.get("id")

            # Busca o ano no nome do recurso
            match = re.search(r"(\d{4})", nome_recurso)
            if not match:
                continue
            ano_recurso = int(match.group(1))

            if ano != ano_recurso:
                continue

            encontrou_ano = True
            arquivo_csv = os.path.join(pasta, f"ENA_{ano}_original.csv")
            arquivo_parquet = os.path.join(pasta, f"ENA_{ano}.parquet")

            # Download pelo resource_id
            if not baixar_por_resource_id(resource_id, arquivo_csv):
                continue

            try:
                print(f"[CONVERSAO] Processando {arquivo_csv}...")

                # Lê CSV robustamente
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

                # Filtra por intervalo de datas
                df_filtrado = df[(df["ena_data"] >= inicio) & (df["ena_data"] <= fim)]
                print(f"[INFO] Após filtro de data: {len(df_filtrado)} linhas")

                if len(df_filtrado) > 0:
                    # Salva como Parquet
                    df_filtrado.to_parquet(arquivo_parquet, index=False)

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

            break  # Para de procurar outros recursos para este ano

        if not encontrou_ano:
            print(f"[AVISO] Não encontrado recurso para o ano {ano}")

    # Relatório final
    print(f"\n{'='*50}")
    print(f"[RELATÓRIO FINAL]")
    print(f"Anos processados: {len(anos)}")
    print(f"Sucessos: {sucessos}")
    print(f"Pasta: {pasta}")

    arquivos_parquet = sorted([f for f in os.listdir(pasta) if f.endswith(".parquet")])
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
    main(data_inicio="2025/01/01", data_fim="2025/09/20")
