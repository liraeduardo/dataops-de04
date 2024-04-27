# Derivando da imagem oficial do MySQL
FROM mysql:8

ENV MYSQL_DATABASE=db
ENV MYSQL_ALLOW_EMPTY_PASSWORD=0
ENV MYSQL_ROOT_PASSWORD=$DB_PASSWORD

EXPOSE 3306

# Adicionando os scripts SQL para serem executados na criação do banco
COPY ./db/ /docker-entrypoint-initdb.d/