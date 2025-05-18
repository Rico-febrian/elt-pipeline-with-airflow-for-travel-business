from airflow.hooks.base import BaseHook
from minio import Minio

class MinioClient:
    def _get():
        minio = BaseHook.get_connection('minio')
        client = Minio(
            endpoint = minio.extra_dejson['endpoint_url'],
            access_key = minio.login,
            secret_key = minio.password,
            secure = False
        )

        return client