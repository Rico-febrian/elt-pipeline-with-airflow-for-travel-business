import pandas as pd
from io import BytesIO
from pangres import upsert
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.minio import MinioClient

class Load:

    @staticmethod
    def load_to_staging(connection_id: str, bucket_name: str, object_name: str, table_name: str, schema_name: str):
        """
        Load data from MinIO to PostgreSQL staging table using pangres.upsert.

        Args:
            connection_id (str): Airflow Connection ID for PostgreSQL.
            bucket_name (str): Name of the MinIO bucket.
            object_name (str): Name of the object in MinIO (tanpa path 'temp/' dan ekstensi '.csv').
            table_name (str): Name of the staging table in PostgreSQL.
            schema_name (str): Schema name for the staging table in PostgreSQL.
        """

        # Define staging tables primary keys
        primary_keys_map = {
            "aircrafts_data": "aircraft_code",
            "airports_data": "airport_code",
            "bookings": "book_ref",
            "tickets": "ticket_no",
            "seats": ["aircraft_code", "seat_no"],
            "flights": "flight_id",
            "ticket_flights": ["ticket_no", "flight_id"],
            "boarding_passes": ["ticket_no", "flight_id"],
        }

        # Initialize MinIO client
        minio_client = MinioClient._get()

        
        object_path = f"/temp/{object_name}.csv"

        try:
            print(f"Fetching object {object_path} from bucket {bucket_name}...")
            response = minio_client.get_object(bucket_name, object_path)
            
            # Load CSV data into Pandas DataFrame
            df = pd.read_csv(BytesIO(response.read()))
            print(f"Successfully read {object_path} into DataFrame. Shape: {df.shape}")

            if df.empty:
                print(f"DataFrame from {object_path} is empty. Nothing to load.")
                return

            # Setup PostgreSQL SQLAlchemy engine using PostgresHook
            print(f"Getting SQLAlchemy engine for connection_id: {connection_id}")
            pg_hook = PostgresHook(postgres_conn_id=connection_id)
            conn_engine = pg_hook.get_sqlalchemy_engine()
            print("SQLAlchemy engine obtained.")

            # Set index based on table primary key(s) for pangres upsert
            if table_name in primary_keys_map:
                pk_columns = primary_keys_map[table_name]
                # Set index to primary key columns
                missing_pk_cols = [col for col in (pk_columns if isinstance(pk_columns, list) else [pk_columns]) if col not in df.columns]
                if missing_pk_cols:
                    raise ValueError(f"Primary key column(s) {missing_pk_cols} not found in DataFrame columns: {df.columns.tolist()}")
                
                df.set_index(pk_columns, inplace=True)
                print(f"DataFrame index set to: {pk_columns}")
            else:
                print(f"Warning: No primary key defined in primary_keys_map for table {table_name}. "
                      "Upsert behavior for 'update' might not work as expected if DataFrame index is not set.")
                # raise ValueError(f"Primary key for table {table_name} not found in primary_keys_map. Cannot perform upsert.")


            # Load DataFrame to PostgreSQL staging table
            print(f"Loading DataFrame to {schema_name}.{table_name} using pangres.upsert...")
            upsert(
                con=conn_engine,
                df=df,
                table_name=table_name,
                schema=schema_name,
                if_row_exists='update',
            )
            
            print(f"Data from {object_path} successfully upserted to {schema_name}.{table_name}")

        except Exception as e:
            print(f"Error during load_to_staging for table '{table_name}' from object '{object_path}':")
            import traceback
            traceback.print_exc()
        finally:
            if 'response' in locals() and response:
                response.close()
                response.release_conn()
