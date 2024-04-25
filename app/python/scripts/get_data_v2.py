from datetime import datetime
import os
import re
import sys
import time

import logging
import uuid
from dotenv import load_dotenv
import mysql.connector
import pandas as pd
import requests

from sqlalchemy import create_engine
from config import configs, project_folders

logging.basicConfig(level=logging.INFO)
load_dotenv()


def collect_data():
    """
    Função para coletar os dados da origem e normalizá-los em um DataFrame pandas.
    """
    try:
        response = requests.get(os.getenv('URL'), timeout=10).json()
        data = pd.json_normalize(response['results'])
        logging.info('Dados coletados na origem')
        logging.info('shape: %s', data.shape)
        return data
    except requests.RequestException as e:
        logging.error('Erro ao coletar os dados: %s', e)
        sys.exit(1)


def configure_folders():
    """
    Função para configurar as pastas do projeto.
    """
    try:
        project_folders(configs)
        logging.info('Pastas configuradas')
    except FileNotFoundError as e:
        logging.error('Erro ao configurar as pastas: %s', e)
        sys.exit(1)


def create_bronze_layer(data):
    """
    Função para criar a camada bronze e salvar os dados em um arquivo CSV.
    """
    try:
        t = datetime.now().strftime('%H_%M_%S')
        filename = f"{configs['bronze_path']}\\{t}_{uuid.uuid4()}.csv"
        data.to_csv(filename, sep=';', index=False)
        logging.info('Camada bronze criada')
    except (PermissionError, FileNotFoundError) as e:
        logging.error('Erro ao criar a camada bronze: %s', e)
        sys.exit(1)


def process_data(data):
    """
    Função para processar os dados, renomear colunas, tratar telefones, celulares, etc.
    """
    try:
        cols = ['gender', 'name.title', 'name.first', 'name.last',
                'location.city', 'location.state', 'location.country', 'nat',
                'email', 'dob.date', 'phone', 'cell']

        cols_renamed = ['sexo', 'titulo', 'nome', 'sobrenome',
                        'cidade', 'estado', 'pais', 'nacionalidade',
                        'email', 'data_nascimento', 'telefone', 'celular']

        data_filtered = data[cols].rename(
            columns={col: ren for col, ren in zip(cols, cols_renamed)}
            )
        logging.info('Colunas selecionadas')

        # Tratamento de telefone e celular
        # Eliminados os parênteses, espaços, letras, zeros iniciais
        padrao = re.compile(r"^00|[()\s\-a-zA-Z]")

        data_filtered['telefone'] = data_filtered['telefone'] \
            .apply(lambda x: str(int(re.sub(padrao, '', x))))
        data_filtered['celular'] = data_filtered['celular'] \
            .apply(lambda x: str(int(re.sub(padrao, '', x))))

        cols = ['nome', 'sobrenome', 'cidade', 'estado', 'pais',
                'nacionalidade', 'email', 'telefone', 'celular']

        padrao = r'[^\w\s]'  # Remove qualquer caractere especial encontrado

        for c in cols:
            data_filtered[c] = data_filtered[c].apply(lambda x: re.sub(padrao, '', x.lower()))

        data_filtered['data_nascimento'] = data_filtered['data_nascimento'] \
            .apply(lambda x: datetime.strptime(x[:10], '%Y-%m-%d'))

        data_filtered['load_date'] = datetime.now().strftime('%Y-%m-%d')

        logging.info('Dados tratados')
        logging.info('shape: %s', data_filtered.shape)
        return data_filtered
    except Exception as e:
        logging.error('Erro ao processar os dados: %s', e)
        sys.exit(1)


def connect_and_save_to_mysql(data):
    """
    Função para conectar ao banco de dados MySQL e salvar os dados na tabela 'cadastro'.
    """
    try:
        engine = create_engine("mysql+mysqlconnector://root:root@mysql/db")
        timeout = 60
        start_time = time.time()

        while time.time() - start_time <= timeout:
            try:
                logging.info('Tentando conexão...')
                time.sleep(20)
                con = mysql.connector.connect(
                    user='root',
                    password='root',
                    host='mysql',
                    port="3306",
                    database='db'
                )
                logging.info('Conectado ao db')
                data.to_sql('cadastro', con=engine, if_exists='append', index=False)
                con.close()
                logging.info('Salvo na camada silver - mysql')
                sys.exit()
            except mysql.connector.Error:
                logging.info('Nova tentativa em alguns segundos...')
                time.sleep(10)
        logging.info('Servidor indisponível.')
    except Exception as e:
        logging.error('Erro ao conectar e salvar dados no MySQL: %s', e)
        sys.exit(1)


def main():
    """
    Função principal para executar todas as etapas do processo.
    """
    try:
        data = collect_data()
        configure_folders()
        create_bronze_layer(data)
        processed_data = process_data(data)
        connect_and_save_to_mysql(processed_data)
    except Exception as e:
        logging.error('Erro ao executar o processo: %s', e)
        sys.exit(1)


if __name__ == "__main__":
    main()
