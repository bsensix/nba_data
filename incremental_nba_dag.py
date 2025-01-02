import time
from datetime import timedelta

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from nba_api.stats.endpoints import commonplayerinfo, playergamelog, teamgamelog
from nba_api.stats.static import players, teams
from sqlalchemy import create_engine

local_tz = pendulum.timezone("America/Sao_Paulo")

# Sets the DAG execution interval
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 10, 17, tz="America/Sao_Paulo"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG description
dag = DAG(
    "NBA_Incremental_DAG",
    default_args=default_args,
    description="Script para baixar os dados dos jogos da NBA do dia aneterior",
    schedule_interval="0 8 * * *",  # Runs daily at 08:00
    catchup=False,
    tags=["NBA", "Incremental"],
)


# Task 1: Data Extract Function Games
def extract_function_games(**kwargs):
    # Função para obter o ID de um time a partir do nome
    def get_team_id(team_name):
        nba_teams = teams.get_teams()
        for team in nba_teams:
            if team["full_name"] == team_name:
                return team["id"]
        return None

    # Função para obter os últimos jogos de um time
    def get_team_last_games(team_id):
        game_log = teamgamelog.TeamGameLog(
            team_id=team_id, season="2024-25", season_type_all_star="Regular Season"
        ).get_data_frames()[0]
        return game_log

    nba_teams = teams.get_teams()
    all_teams_games = pd.DataFrame()

    for team in nba_teams:
        team_id = team["id"]

        team_games = get_team_last_games(team_id)
        all_teams_games = pd.concat([all_teams_games, team_games], ignore_index=True)

        time.sleep(2)

    results = all_teams_games
    results_json = results.to_json(orient="records")
    kwargs["ti"].xcom_push(key="df_bruto", value=results_json)


# Define a Task 1
extract_function_task_1 = PythonOperator(
    task_id="exctract_funcion_games",
    python_callable=extract_function_games,
    provide_context=True,
    dag=dag,
)


# Task 2: Transformation Function
def transformation_function(**kwargs):
    d = kwargs["ti"].xcom_pull(task_ids="exctract_funcion_games", key="df_bruto")
    games_results = pd.read_json(d, orient="records")

    games_results["Game_ID"].astype("Int64")
    games_results["GAME_DATE"] = pd.to_datetime(games_results["GAME_DATE"])
    games_results["TEAM_1"] = games_results["MATCHUP"].str[:3]
    games_results["TEAM_2"] = games_results["MATCHUP"].str[-3:]

    yesterday = pd.Timestamp.now(tz=local_tz) - pd.Timedelta(days=1)
    games_results = games_results[
        games_results["GAME_DATE"].dt.date == yesterday.date()
    ]

    games = games_results.drop(
        columns=[
            "Team_ID",
            "MATCHUP",
            "W",
            "L",
            "W_PCT",
            "MIN",
            "FGM",
            "OREB",
            "DREB",
            "WL",
            "FGA",
            "FG_PCT",
            "FG3M",
            "FG3A",
            "FG3_PCT",
            "FTM",
            "FTA",
            "FT_PCT",
            "OREB",
            "DREB",
            "REB",
            "AST",
            "STL",
            "TOV",
            "BLK",
            "PF",
            "PTS",
        ]
    )
    games.drop_duplicates(subset=["Game_ID"], keep="first", inplace=True)

    games_json = games.to_json(orient="records")
    kwargs["ti"].xcom_push(key="df_games", value=games_json)

    games_results_json = games_results.to_json(orient="records")
    kwargs["ti"].xcom_push(key="df_games_results", value=games_results_json)


# Define a Task 2
transformation_function_task_2 = PythonOperator(
    task_id="transformation_function",
    python_callable=transformation_function,
    provide_context=True,
    dag=dag,
)


# Task 3: Excract Function Players
def exctract_funcion_players_results(**kwargs):
    # Função para obter o log de jogos de um jogador específico
    def get_player_game_log(player_id):
        game_log = playergamelog.PlayerGameLog(player_id=player_id, season="2024-25")
        game_log_data = game_log.get_data_frames()[0]
        return game_log_data

    # Criando listas para armazenar os dados
    player_ids = []
    player_names = []
    game_logs_dataframes = []

    # Obtendo todos os jogadores atuais da NBA
    all_players = players.get_players()
    active_players = [player for player in all_players if player["is_active"]]
    active_players_df = pd.DataFrame(active_players)

    # DataFrame para armazenar todos os logs de jogos
    all_player_game_logs = pd.DataFrame()

    # Iterando sobre os jogadores e obtendo os logs de jogos
    for player in active_players:
        player_id = player["id"]

        try:
            # Obtendo informações detalhadas sobre o jogador
            player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            player_team = player_info.get_data_frames()[0]["TEAM_ABBREVIATION"].values[
                0
            ]

            # Obtendo o log de jogos do jogador
            player_game_log = get_player_game_log(player_id)

            # Exibindo informações sobre o jogador e seus últimos jogos
            player_name = f"{player['full_name']} ({player_team})"

            # Armazenando os dados nas listas
            player_ids.append(player_id)
            player_names.append(player_name)
            game_logs_dataframes.append(player_game_log)

            # Concatenando o log de jogos do jogador ao DataFrame principal
            all_player_game_logs = pd.concat(
                [all_player_game_logs, player_game_log], ignore_index=True
            )

        except Exception as e:
            print(f"\nErro ao obter dados do jogador {player['full_name']}: {str(e)}")
        time.sleep(1)

    # Filtrando os dados para obter apenas os jogos de ontem
    yesterday = pd.Timestamp.now(tz=local_tz) - pd.Timedelta(days=1)
    all_player_game_logs["GAME_DATE"] = pd.to_datetime(
        all_player_game_logs["GAME_DATE"]
    )
    all_player_game_logs = all_player_game_logs[
        all_player_game_logs["GAME_DATE"].dt.date == yesterday.date()
    ]

    all_player_game_logs_json = all_player_game_logs.to_json(orient="records")
    kwargs["ti"].xcom_push(key="df_players", value=all_player_game_logs_json)

    active_players_df_json = active_players_df.to_json(orient="records")
    kwargs["ti"].xcom_push(key="df_active_players", value=active_players_df_json)


# Define a Task 3
exctract_funcion_players_result_task_3 = PythonOperator(
    task_id="exctract_funcion_players_results",
    python_callable=exctract_funcion_players_results,
    provide_context=True,
    dag=dag,
)

# connection variables
password = Variable.get("PASSWORD_POSTGRES")
user = Variable.get("USER_POSTGRES")
host = Variable.get("HOST_POSTGRES")
port = Variable.get("PORT_POSTGRES")
database = Variable.get("DATABASE_POSTGRES")

# data loading function
engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)


# Function to load data into the database
def load_function_generic(
    engine, df_key, table_name, task_id, schema="NBA", max_rows=16384, **kwargs
):
    df = kwargs["ti"].xcom_pull(task_ids=task_id, key=df_key)
    df = pd.read_json(df, orient="records")

    partes = [df[i : i + max_rows] for i in range(0, len(df), max_rows)]

    for parte in partes:
        parte.to_sql(
            name=table_name, con=engine, schema=schema, if_exists="append", index=False
        )


# Task 3: Load Function Games
load_function_task_3 = PythonOperator(
    task_id="load_function_games",
    python_callable=load_function_generic,
    op_kwargs={
        "engine": engine,
        "df_key": "df_games",
        "table_name": "GAMES",
        "task_id": "transformation_function",
    },
    provide_context=True,
    dag=dag,
)

# Task 4: Load Function Games Results
load_function_task_4 = PythonOperator(
    task_id="load_function_games_results",
    python_callable=load_function_generic,
    op_kwargs={
        "engine": engine,
        "df_key": "df_games_results",
        "table_name": "GAMES_RESULTS",
        "task_id": "transformation_function",
    },
    provide_context=True,
    dag=dag,
)

# Task 5: Load Function Active Players
load_function_task_5 = PythonOperator(
    task_id="load_function_active_players",
    python_callable=load_function_generic,
    op_kwargs={
        "engine": engine,
        "df_key": "df_active_players",
        "table_name": "PLAYERS",
        "task_id": "exctract_funcion_players_results",
    },
    provide_context=True,
    dag=dag,
)

# Task 6: Load Function Players Results
load_function_task_6 = PythonOperator(
    task_id="load_function_active_players_results",
    python_callable=load_function_generic,
    op_kwargs={
        "engine": engine,
        "df_key": "df_players",
        "table_name": "PLAYER_RESULTS",
        "task_id": "exctract_funcion_players_results",
    },
    provide_context=True,
    dag=dag,
)

# Defines the dependency between tasks
(
    extract_function_task_1
    >> transformation_function_task_2
    >> exctract_funcion_players_result_task_3
    >> [
        load_function_task_3,
        load_function_task_4,
        load_function_task_5,
        load_function_task_6,
    ]
)
