from helper.minio import MinioClient
from helper.postgres import Execute
from io import BytesIO
import pandas as pd
import datetime  

class Extract:
    @staticmethod
    def source_db(connection_id, query_path, bucket_name, table_name):
        
        # Get data from source db
        df = Execute._get_dataframe(
            connection_id=connection_id,
            query_path=query_path
        )
        
        # Set up MinIO client
        client = MinioClient._get()
        
        # Check and create MinIO bucket if not exist 
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        
        # Convert DataFrame to CSV
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)
        
        # Load converted data to selected MinIO bucket
        client.put_object(
            bucket_name=bucket_name,
            object_name=f"/temp/{table_name}.csv",
            data=csv_buffer,
            length=len(csv_bytes),
            content_type='application/csv'
        )
        
        print(f"Data uploaded to Minio bucket '{bucket_name}' as '{table_name}.csv'")
