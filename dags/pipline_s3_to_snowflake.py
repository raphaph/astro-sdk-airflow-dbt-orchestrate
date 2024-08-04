from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

from airflow.models import DAG

from datetime import datetime

S3_FILE_PATH="s3://astro-s3-learning/astro-files" # S3://bucket/folder/
S3_CONN_ID="aws_default" # conexão ja definida
SNOWFLAKE_CONN_ID="snowflake_default" # conexão ja definida
SNOWFLAKE_TABLE="tb_astro_category" # nome da tabela a ser criada ou sobrescrita

dag = DAG(
    dag_id="pipeline_s3_snowflake",
    start_date=datetime(2024,8,4),
    schedule="@daily"
)

with dag:
    category_data = aql.load_file(
    input_file=File( # entrada do arquivo
        path=S3_FILE_PATH + "/product_category.csv", conn_id=S3_CONN_ID
    ),
    output_table=Table( # saida em formato tabela no snowflake
        name=SNOWFLAKE_TABLE, conn_id=SNOWFLAKE_CONN_ID
        ),
    if_exists="replace"
)
    
# finish