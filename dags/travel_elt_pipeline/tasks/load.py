from helper.minio import MinioClient
from helper.logger import logger
from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import BytesIO
import pandas as pd
import json

logger = logger(logger_name="load-job")

class Load:
    @staticmethod
    def load_to_staging(connection_id, bucket_name, table_name, schema):
        """
        Load CSV data from a MinIO bucket into a staging table in a PostgreSQL database.

        Args:
            connection_id (str): Airflow connection ID for the PostgreSQL staging database.
            bucket_name (str): Name of the MinIO bucket containing the CSV file.
            table_name (str): Name of the table (and CSV file) to load data into.
            schema (str): Database schema name where the staging table exists.

        Returns:
            None: This function uploads data to the staging table but does not return a value.

        Raises:
            Exception: If any operation fails during the loading process (e.g., connection errors, data upload failures).
        """
        
        try:
            # Initialize MinIO client and fetch the CSV object from the specified bucket
            logger.info('Connecting to MinIO...')
            client = MinioClient._get()

            object_path = f"/temp/{table_name}.csv"
            logger.info(f"Fetching object from MinIO: {bucket_name}{object_path}")
            
            obj = client.get_object(
                bucket_name=bucket_name,
                object_name=object_path
            )
            
            # Read the CSV content from the MinIO object as bytes, convert to a pandas DataFrame
            logger.info('Reading CSV content into DataFrame...')
            df = pd.read_csv(BytesIO(obj.read()))
            
            # Close the MinIO object stream to free resources
            obj.close()
            logger.info('CSV loaded successfully!')

            # Convert specific columns to JSON string format for JSONB compatibility in Postgres
            jsonb_columns = ['model', 'airport_name', 'city', 'contact_data']
            logger.info(f'Converting selected columns to JSON strings (if exists)')

            for column_name in jsonb_columns:
                if column_name in df.columns:
                    # Convert each row's dictionary/list to a JSON-formatted string
                    df[column_name] = df[column_name].apply(json.dumps)

            logger.info('JSON columns converted')
            
            # Create a SQLAlchemy engine from Airflow PostgresHook to connect to the staging DB
            logger.info('Connecting to PostgreSQL via SQLAlchemy engine...')
            hook = PostgresHook(postgres_conn_id=connection_id)
            engine = hook.get_sqlalchemy_engine()
            
            # Prepare SQL command to truncate the staging table and cascade constraints
            truncate_query = f"TRUNCATE TABLE {schema}.{table_name} CASCADE;"

            logger.info(f'Truncating table {schema}.{table_name}...')
            
            # Execute the truncate query within a transactional block to ensure atomicity
            with engine.connect() as connection:
                with connection.begin():
                    connection.execute(truncate_query)
            logger.info('Table truncated successfully!')
            
            # Upload DataFrame contents into the staging table, appending data without DataFrame index
            logger.info(f'Uploading DataFrame to table {schema}.{table_name}...')
            df.to_sql(
                name=table_name,
                schema=schema,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            
            logger.info(f'Data loaded into staging table {schema}.{table_name}')

        except Exception as e:
            logger.error(f"Error during loading CSV data from MinIO to staging schema in DWH: {e}", exc_info=True)
            raise