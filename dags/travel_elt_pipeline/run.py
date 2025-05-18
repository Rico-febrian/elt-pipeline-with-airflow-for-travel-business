from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from pendulum import datetime
from travel_elt_pipeline.tasks.extract import Extract
from travel_elt_pipeline.tasks.load import Load

@dag(
    dag_id='travel_elt_pipeline',
    start_date=datetime(2025, 5, 18),
    schedule="@daily",
    catchup=False,
)
def travel_elt_pipeline():

    source_tables = [
        "aircrafts_data",
        "airports_data",
        "bookings",
        "tickets",
        "seats",
        "flights",
        "ticket_flights",
        "boarding_passes"
    ]

    connection_id_extract = "source_db"
    connection_id_load = "staging_db"
    bucket_name = "extracted-data"
    schema_name = "stg"

    @task_group
    def extract_group():
        tasks = []
        for table_name in source_tables:
            task = PythonOperator(
                task_id=f"extract_{table_name}",
                python_callable=Extract.source_db,
                op_kwargs={
                    "connection_id": connection_id_extract,
                    "query_path": f"travel_elt_pipeline/query/{table_name}.sql",
                    "bucket_name": bucket_name,
                    "table_name": table_name,
                },
            )
            tasks.append(task)
        return tasks

    @task_group
    def load_group():
        prev_task = None
        for table_name in source_tables:
            task = PythonOperator(
                task_id=f"load_{table_name}",
                python_callable=Load.load_to_staging,
                op_kwargs={
                    "connection_id": connection_id_load,
                    "bucket_name": bucket_name,
                    "object_name": table_name,
                    "table_name": table_name,
                    "schema_name": schema_name,
                },
            )
            if prev_task:
                prev_task >> task
            prev_task = task
        return prev_task

    extract_tasks = extract_group()
    load_tasks = load_group()

    if isinstance(extract_tasks, list):
        for t in extract_tasks:
            t >> load_tasks
    else:
        extract_tasks >> load_tasks

travel_elt_pipeline()
