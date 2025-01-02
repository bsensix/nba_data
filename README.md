# Repositório de Dados da NBA

Este repositório contém scripts e arquivos necessários para extrair, transformar e carregar dados de jogos e jogadores da NBA em um banco de dados PostgreSQL hospedado na AWS, utilizando Apache Airflow como orquestrador.

## Estrutura do Projeto

- [`schema.png`](#file:schema.png-context): Imagem que representa o esquema do banco de dados PostgreSQL, incluindo as relações e restrições definidas nos scripts SQL.
- [`incremental_nba_dag.py`](#file:incremental_nba_dag.py-context): DAG do Airflow que automatiza o processo de ETL (Extração, Transformação e Carga) dos dados de jogos e jogadores da NBA.
- [`relationship_players.sql`](#file:relationship_players.sql-context): Script SQL que define as relações e restrições entre as tabelas `PLAYERS` e `PLAYER_RESULTS`.
- [`relationship_games.sql`](#file:relationship_games.sql-context): Script SQL que define as relações e restrições entre as tabelas `GAMES` e `GAMES_RESULTS`.
- [`create_table_games.sql`](#file:create_table_games.sql-context): Script SQL para criar a tabela `GAMES`.
- [`create_table_playersresults.sql`](#file:create_table_playersresults.sql-context): Script SQL para criar a tabela `PLAYER_RESULTS`.
- [`create_table_players.sql`](#file:create_table_players.sql-context): Script SQL para criar a tabela `PLAYERS`.
- [`create_table_gamesresults.sql`](#file:create_table_gamesresults.sql-context): Script SQL para criar a tabela `GAMES_RESULTS`.

## Descrição dos Arquivos


### incremental_nba_dag.py

Este arquivo contém a DAG do Airflow que automatiza o processo de ETL dos dados da NBA. A DAG é composta pelas seguintes tarefas:

- `extract_function_games()`: Extrai os dados dos últimos jogos de todos os times da NBA.
- `transformation_function()`: Transforma os dados extraídos, limpando e estruturando-os.
- `exctract_funcion_players_results()`: Extrai os logs de jogos dos jogadores ativos da NBA.
- `load_function_generic()`: Carrega os dados transformados no banco de dados PostgreSQL.

