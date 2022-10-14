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
def dag_01():

    @task
    def get_data():
        NOME_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_ARQUIVO, index=False, sep=";")
        return NOME_ARQUIVO

    @task
    def calculate_passangers(NOME_ARQUIVO):
        NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(NOME_ARQUIVO, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg(
            {"PassengerId": "count"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def calculate_ticket(NOME_ARQUIVO):
        NOME_TABELA2 = "/tmp/ticket_por_sexo_classe.csv"
        df = pd.read_csv(NOME_ARQUIVO, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({"Fare": "mean"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA2, index=False, sep=";")
        return NOME_TABELA2

    @task
    def calculate_sibsp(NOME_ARQUIVO):
        NOME_TABELA3 = "/tmp/sibsp_parch_sexo_classe.csv"
        df = pd.read_csv(NOME_ARQUIVO, sep=";")
        df['sibsp_parch'] = df['SibSp'] + df['Parch']
        res = df.groupby(['Sex', 'Pclass']).agg(
            {"sibsp_parch": "count"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA3, index=False, sep=";")
        return NOME_TABELA3

    @task
    def join_data(NOME_TABELA, NOME_TABELA2, NOME_TABELA3):
        TABELA_UNICA = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_TABELA, sep=";")
        df1 = pd.read_csv(NOME_TABELA2, sep=";")
        df2 = pd.read_csv(NOME_TABELA3, sep=";")

        df3 = df.merge(df1, on=['Sex', 'Pclass'], how='inner')
        df4 = df2.merge(df3, on=['Sex', 'Pclass'], how='inner')
        print(df4)
        df4.to_csv(TABELA_UNICA, index=False, sep=";")
        return TABELA_UNICA

    inicio = DummyOperator(task_id="inicio")
    fim = DummyOperator(task_id="fim")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def end():
        print("Terminou")
    triggerdag = TriggerDagRunOperator(
        task_id="Init_dag_02",
        trigger_dag_id="dag_02")

    df = get_data()
    indicador = calculate_passangers(df)
    ticket = calculate_ticket(df)
    sibsp_parch = calculate_sibsp(df)
    tab_final = join_data(indicador, ticket, sibsp_parch)

    inicio >> df >> [indicador, ticket,
                     sibsp_parch] >> tab_final >> fim >> triggerdag


execucao = dag_01()
