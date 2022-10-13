from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="static_loops",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    catchup=False
):

    start = EmptyOperator(task_id="start")
    midpoint = EmptyOperator(task_id="midpoint")
    end = EmptyOperator(task_id="end")

    @task
    def add_19_task_flow(x):
        return x + 19

    def add_19(x):
        return x + 19

    for i in range(5):

        my_traditional_task = PythonOperator(
            task_id=f"my_traditional_task{i}",
            python_callable=add_19,
            op_args=[i]
        )

        start >> add_19_task_flow(i) >> midpoint

        start >> my_traditional_task >> midpoint

    
    extract_normal = PythonOperator(
            task_id="extract_normal",
            python_callable=add_19,
            op_args=[i]
    )

    midpoint >> extract_normal >> end

    def add_19(x):
        return x + 19

    def add_23(x):
        return x + 23

    extract_dynamic = PythonOperator.partial(
            task_id="extract_dynamic",
    ).expand(op_args=[[0],[1],[2]], python_callable=[add_19, add_23],pool=["a"])


    @task
    def add_19_task_flow(x):
        return x + 19

    add_19_task_flow.partial().expand(x=[0,1,2])
    

    midpoint >> extract_dynamic >> end
