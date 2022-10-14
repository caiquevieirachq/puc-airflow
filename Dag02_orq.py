import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Caique Vieira",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 9)
}


@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Trabalho Titanic'])
def dag_02():

    @task
    def join_tabela_unica():
        NOME_DO_ARQUIVO = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def calculate_indicator(NOME_DO_ARQUIVO):
        TABELA_INDICADORES = "/tmp/resultados.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        res = df.groupby(['Sex']).agg({"Fare":"mean", "sibsp_parch":"mean"}).reset_index()
        print(res)
        res.to_csv(TABELA_INDICADORES, index=False, sep=";")
        return TABELA_INDICADORES

    start = DummyOperator(task_id="inicio")
    fim = DummyOperator(task_id="fim")

    df_unica = join_tabela_unica()
    df_indicadores = calculate_indicator(df_unica)

    start >> df_unica >> df_indicadores >> fim


execucao = dag_02()
