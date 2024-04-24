USE db;

CREATE TABLE cadastro (
    id integer not null auto_increment,
    sexo varchar(200),
    titulo varchar(200),
    nome varchar(200),
    sobrenome varchar(200),
    cidade varchar(200),
    estado varchar(200),
    pais varchar(200),
    nacionalidade varchar(200),
    email varchar(200),
    data_nascimento date,
    telefone varchar(200),
    celular varchar(200),
    load_date datetime not null,
    KEY (id)
);

SET character_set_client = utf8mb4;
SET character_set_connection = utf8mb4;
SET character_set_results = utf8mb4;
SET collation_connection = utf8mb4_general_ci;