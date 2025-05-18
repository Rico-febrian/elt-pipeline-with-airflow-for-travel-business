from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
BASE_PATH = "/opt/airflow/dags"


class Execute:
    def _query(connection_id, query_path):
        hook = PostgresHook(postgres_conn_id = connection_id)
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        cursor.execute(query)
        cursor.close()
        connection.commit()
        connection.close()

    def _get_dataframe(connection_id, query_path):
        pg_hook = PostgresHook(postgres_conn_id = connection_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        cursor.execute(query)
        result = cursor.fetchall()
        column_list = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns = column_list)

        cursor.close()
        connection.commit()
        connection.close()
        
        return df


    def _insert_dataframe(connection_id, query_path, dataframe):
        BASE_PATH = "/opt/airflow/dags"
        
        pg_hook = PostgresHook(postgres_conn_id = connection_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        
        with open(f'{BASE_PATH}/{query_path}', 'r') as file:
            query = file.read()

        for index, row in dataframe.iterrows():
            record = row.to_dict()
            pg_hook.run(query, parameters = record)

        cursor.close()
        connection.commit()
        connection.close()