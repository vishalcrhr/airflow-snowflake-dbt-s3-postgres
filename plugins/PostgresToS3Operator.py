from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class PostgresToS3Operator(BaseOperator):
    """
    Execute a COPY statement against a Redshift Database.
    :params pre_sql: Any SQL statement or sql file to be executed
    before the COPY statement.
    """
    
    # Declare fields that can be templated
    template_fields = ("pre_sql", "s3_bucket",  "copy_options")
    template_ext = (".hql", ".sql")

    def __init__(
        self,
        tablename,
        s3_bucket,
        aws_con_id,
        postgres_conn_id,
        pre_sql=None,
        copy_options="""
                TIMEFORMAT AS 'epochmillisecs'
                BLANKSASNULL
                EMPTYASNULL
                FORMAT AS AVRO 'auto'
                GZIP
            """,
        *args,
        **kwargs
    ):
        self.tablename = tablename
        self.s3_bucket = s3_bucket
        self.aws_con_id = aws_con_id
        self.postgres_conn_id = postgres_conn_id
        self.pre_sql = pre_sql
        self.copy_options = copy_options
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        connection = self.postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(self.pre_sql)

        import pandas as pd
        headers = list(map(lambda t:t[0], cursor.description))

        df = pd.DataFrame(cursor.fetchall(), columns =headers)
        self.log.info(f"""df is {df.head()} """)
        
        s3_conn = S3Hook(aws_conn_id = self.aws_con_id)
        self.log.info(f"""{self.s3_bucket}""")
        s3_conn.load_string(df.to_csv(index=False), key='xyz.csv', bucket_name = self.s3_bucket,replace=True)
        
