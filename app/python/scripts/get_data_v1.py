import requests
import pandas as pd
import re
import uuid
from datetime import datetime
from dotenv import load_dotenv
import logging
from config import configs, project_folders
import os

logging.basicConfig(level=logging.INFO)
load_dotenv()

response = requests.get(os.getenv('URL'), timeout=10).json()
data = pd.json_normalize(response['results'])
logging.info('Dados coletados na origem')
logging.info(f'shape: {data.shape}')

project_folders(configs)
configs
logging.info('Pastas configuradas')

filename = f"{configs['bronze_path']}\\{datetime.now().strftime('%H_%M_%S')}_{uuid.uuid4()}.csv"
data.to_csv(filename, sep=';', index=False)
logging.info('camada bronze criada')

cols = ['gender', 'name.title', 'name.first', 'name.last', 
        'location.city', 'location.state', 'location.country', 'nat', 
        'email', 'dob.date', 'phone', 'cell' ]

cols_renamed = ['sexo', 'titulo', 'nome', 'sobrenome', 
                'cidade', 'estado', 'pais', 'nacionalidade', 
                'email', 'data_nascimento', 'telefone', 'celular' ]

data_filtered = data[cols].rename(columns= {col:ren for col, ren in zip(cols, cols_renamed)} )
logging.info('Colunas selecionadas')

# tratamento de telefone e celular
# eliminados os parenteses, espaços, letras, zeros iniciais
padrao = re.compile(r"^00|[()\s\-a-zA-Z]")

data_filtered['telefone'] = data_filtered['telefone'].apply(lambda x: str(int(re.sub(padrao, '', x))) )
data_filtered['celular'] = data_filtered['celular'].apply(lambda x: str(int(re.sub(padrao, '', x))) )

cols = ['nome', 'sobrenome', 'cidade', 'estado', 'pais',
       'nacionalidade', 'email', 'telefone', 'celular']

padrao = r'[^\w\s]' #remove qualquer caractere especial encontrado

for c in cols:
    data_filtered[c] = data_filtered[c].apply(lambda x: re.sub(padrao, '', x.lower()) )

data_filtered['data_nascimento'] = data_filtered['data_nascimento'] \
    .apply(lambda x: datetime.strptime(x[:10], '%Y-%m-%d') )

data_filtered['load_date'] = datetime.now().strftime('%Y-%m-%d')

logging.info('Dados tratados')

logging.info(f'shape: {data_filtered.shape}')

from sqlalchemy import create_engine
import mysql.connector
import time
import sys

engine = create_engine("mysql+mysqlconnector://root:root@mysql/db")
timeout = 60
start_time = time.time()

while time.time() - start_time <= 60:
    try:
        logging.info('tentando conexão...')    
        time.sleep(20)
        con = mysql.connector.connect(
            user='root', 
            password='root', 
            host='mysql', 
            port="3306", 
            database='db'
            )
        logging.info('Conectado ao db')
        data_filtered.to_sql('cadastro', con=engine, if_exists='append', index=False)
        con.close()
        logging.info('Salvo na camada silver - mysql')
        sys.exit()
    except Exception as e:
        logging.info(e)
        logging.info('Nova tentativa em alguns segundos...')
        time.sleep(10)

logging.info('Servidor indisponível.')