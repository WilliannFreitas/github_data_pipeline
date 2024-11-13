import os
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime
import logging

# Configurações de logging
logging.basicConfig(filename="process_log.log", level=logging.INFO, format="%(asctime)s - %(message)s")
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# Função para carregar variáveis de ambiente e conectar ao banco de dados Postgres
def database_connection():
    load_dotenv()  # Carregar variáveis do arquivo .env
    PostgresURL = os.getenv("PostgresURL")  # Carregar a URL completa do banco de dados
    if not PostgresURL:
        logging.error(f"{now} - A URL de conexão com o banco de dados não foi encontrada no .env")
        raise ValueError("A URL de conexão com o banco de dados não foi encontrada no .env")

    # Criando a conexão com o banco de dados
    engine = create_engine(PostgresURL)
    print(f"{now} - Conexão com o banco de dados bem-sucedida!")
    logging.info(f"{now} - Conexão com o banco de dados bem-sucedida!")
    return engine


# Função para buscar dados de usuários do GitHub
def fetch_github_users(numero_de_usuarios=100, usuarios_por_pagina=100):
    token = os.getenv("GITHUB_TOKEN")  # Carregar o token do GitHub do .env
    if not token:
        logging.error(f"{now} - Token do GitHub não foi encontrado no .env")
        raise ValueError("Token do GitHub não foi encontrado no .env")

    headers = {
        "Authorization": f"Bearer {token}"
    }

    lista_usuarios = []
    pagina_inicial = 1

    while len(lista_usuarios) < numero_de_usuarios:
        url = f"https://api.github.com/users?since={pagina_inicial}&per_page={usuarios_por_pagina}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            dados_usuarios = response.json()

            for usuario in dados_usuarios:
                url_usuario = usuario["url"]
                response_usuario = requests.get(url_usuario, headers=headers)

                if response_usuario.status_code == 200:
                    dados_detalhados = response_usuario.json()
                    lista_usuarios.append({
                        "Login": dados_detalhados["login"],
                        "ID": dados_detalhados["id"],
                        "Nome": dados_detalhados.get("name"),
                        "Empresa": dados_detalhados.get("company"),
                        "Localização": dados_detalhados.get("location"),
                        "Email": dados_detalhados.get("email"),
                        "Bio": dados_detalhados.get("bio"),
                        "Repositórios Públicos": dados_detalhados["public_repos"],
                        "Seguidores": dados_detalhados["followers"],
                        "Seguindo": dados_detalhados["following"],
                        "Criado em": dados_detalhados["created_at"],
                        "Última atualização": dados_detalhados["updated_at"],
                        "URL Perfil": dados_detalhados["html_url"]
                    })

                    if len(lista_usuarios) >= numero_de_usuarios:
                        break
        else:
            print(f"{now} - Erro: {response.status_code}")
            logging.error(f"{now} - Erro: {response.status_code}")
            break

        pagina_inicial += usuarios_por_pagina

    df_usuarios = pd.DataFrame(lista_usuarios)
    print(f"{now} - Coleta de dados concluída!")
    logging.info(f"{now} - Coleta de dados concluída!")
    return df_usuarios


# Função para inserir dados no banco de dados
def insert_into_database(engine, df, table_name, schema_name):
    if df.empty:
        print(f"{now} - DataFrame para {table_name} está vazio. Nenhuma inserção realizada.")
        logging.warning(f"{now} - DataFrame para {table_name} está vazio. Nenhuma inserção realizada.")
        return

    df['data_hora_insercao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df.to_sql(table_name, engine, if_exists='replace', index=False, schema=schema_name)
    print(f"{now} - Dados inseridos com sucesso na tabela {table_name}")
    logging.info(f"{now} - Dados inseridos com sucesso na tabela {table_name}")


# Função principal para executar o processo completo
def main():
    schema_name = "public"
    table_name = "stage_usuarios_github"

    engine = database_connection()

    df_usuarios = fetch_github_users(numero_de_usuarios=1000, usuarios_por_pagina=100)

    insert_into_database(engine, df_usuarios, table_name, schema_name)


if __name__ == "__main__":
    main()
