# Projeto de Dataops

Este é um projeto que tem como objetivo obter dados de uma API, realizar um tratamento nesses dados e enviá-los para um servidor MySQL. O projeto é executado no Docker e utiliza o GitHub Actions para avaliar o código Python usando o Pylint. Se a nota do Pylint for maior igual a 7, a imagem Python é enviada para o Docker Hub.

## Funcionalidades

- Obtenção de dados de uma API.
- Tratamento dos dados obtidos.
- Envio dos dados tratados para um servidor MySQL.
- Execução do projeto no Docker.
- Avaliação do código Python usando Pylint.
- Envio automático da imagem Python para o Docker Hub.

## Pré-requisitos

- Docker instalado e configurado.
- Conta no Docker Hub para enviar a imagem Docker.
- Conta no GitHub para configurar as ações do GitHub.
- Confgurar as seguintes variaveis de ambiente:
    -- URL
    -- DB_PASSWORD
- Configurar as credenciais do Docker Hub:
    -- DOCKER_USERNAME
    -- DOCKER_PASSWORD
  
