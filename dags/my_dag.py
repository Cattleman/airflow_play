# Airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
from datetime import datetime
from datetime import timedelta

# DS stack
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# Set up dfault args

default_args = {
        'owner': 'meemoo',
        'depends_on_past': False,
        'start_date': datetime(2018, 9, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5),
        }


# Sets of callables used by python operator

def prep_data() -> pd.DataFrame:

    _df = pd.DataFrame({"col1": [1,2,3], "col2": [2,4,6]})

    _df.to_csv("../data/raw.csv")

def data_process_double()-> pd.DataFrame:

    _df = pd.read_csv("../data/raw.csv")

    _df["col3"] = _df["col1"] * 2
    _df["col4"] = _df["col2"] * 2

    _df.to_csv("../data/processed.csv")


def save_out_plot() -> None:

    plt.scatter(df["col1"],df["co2"], c='r', label="col1,2")
    plt.scatter(df["col3"], df["col4"], c='b', label="col3,4")
    plt.legend()
    plt.savefig(f'../plots/my_plot.png')


with DAG('simple_plot_dag',
        default_args=default_args,
        schedule_interval='*/2 * * * *'
        ) as dag:
    opr_prep_data = PythonOperator(
        task_id='prep_data',
        python_callable=prep_data
        )

    opr_data_process = PythonOperator(
        task_id='data_process_double',
        python_callable=data_process_double
        )

    opr_save_plot = PythonOperator(
        task_id='save_plot',
        python_callable=save_out_plot
        )

    opr_prep_data >> opr_data_process >> opr_save_plot

