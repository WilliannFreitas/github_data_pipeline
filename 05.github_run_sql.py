import os
import psycopg2
import logging
from dotenv import load_dotenv
from datetime import datetime

now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# URL completa de conexão com o PostgreSQL
postgres_url = os.getenv('PostgresURL')
if not postgres_url:
    message = f"{now} - ERROR - A URL de conexão com o banco de dados não foi encontrada no .env"
    logging.error(message)
    print(message)
    raise ValueError("A URL de conexão com o banco de dados não foi encontrada no .env")

# Diretório contendo os arquivos .sql ou .txt
directory_path = r'C:\repos\github_data_pipeline'


# Função para executar queries em um arquivo
def execute_sql_file(cursor, file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        query = file.read()
        cursor.execute(query)

# Função para processar todos os arquivos SQL/TXT no diretório
def process_files_in_directory(cursor, directory_path):
    files = sorted([f for f in os.listdir(directory_path) if f.endswith('.sql') or f.endswith('.txt')])

    # Iterar sobre cada arquivo no diretório
    for filename in files:
        file_path = os.path.join(directory_path, filename)
        try:
            # Executar o conteúdo do arquivo
            execute_sql_file(cursor, file_path)
            message = f"{now} - INFO - Conteúdo do arquivo {filename} executado com sucesso."
            logging.info(message)
            print(message)

        except psycopg2.Error as e:
            message = f"{now} - ERROR - Erro ao processar {filename}: {e}"
            logging.error(message)
            print(message)
            cursor.connection.rollback()

# Função principal para conectar ao banco e rodar os scripts
def run_scripts():
    try:
        conn = psycopg2.connect(postgres_url)
        cursor = conn.cursor()
        message = f"{now} - INFO - Conectado ao banco de dados com sucesso."
        logging.info(message)
        print(message)

        try:
            process_files_in_directory(cursor, directory_path)
            conn.commit()
            message = f"{now} - INFO - Todas as transações foram confirmadas com sucesso."
            logging.info(message)
            print(message)

        except Exception as e:
            message = f"{now} - ERROR - Ocorreu um erro durante a execução dos scripts: {e}"
            logging.error(message)
            print(message)
            conn.rollback()

        finally:
            cursor.close()
            conn.close()
            message = f"{now} - INFO - Conexão com o banco de dados encerrada."
            logging.info(message)
            print(message)

    except Exception as e:
        message = f"{now} - ERROR - Não foi possível conectar ao banco de dados: {e}"
        logging.error(message)
        print(message)

# Executar a função principal
if __name__ == "__main__":
    run_scripts()
