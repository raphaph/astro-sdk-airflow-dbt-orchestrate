from airflow.models import DAG
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

from pendulum import datetime
import os

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

VARIABLE = "var_value"

#DBT
# O caminho para o projeto dbt
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt-dw-etl/project-etl"
# O caminho onde o Cosmos encontrará o executável do dbt
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"  # no ambiente virtual criado no Dockerfile
# mudar a depender da necessidade e teste f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/activate"

# MODELS
MODEL_TO_QUERY = "stg_product_category" # 1 modelo
MODELS_TO_RUN = "stg_product_category, tru_product_category" # 2 ou mais

# S3 
S3_FILE_PATH="s3://astro-s3-learning/astro-files" # S3://bucket/folder/
S3_CONN_ID="aws_default" # conexão ja definida
SNOWFLAKE_CONN_ID="snowflake_default" # conexão ja definida
SNOWFLAKE_TABLE="tb_astro_category" # nome da tabela a ser criada ou sobrescrita

# cria o profile.yml com base nas info da connection do snowflake
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id = 'snowflake_default'
    )
)

# local de execução do dbt como venv
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

dag = DAG(    
    dag_id = "pipeline_s3_snowflake_dbt_run",
    start_date = datetime(2024, 8, 4),
    schedule = None,
    catchup = False,
    # params={"VARIAVEL": VAR_DEFINED}, # Utilizamos em caso de passar a variavel diretamente pelo airflow no trigger
    tags = ["snowflake","etl"]
    )

with dag:
    
    extract_data = aql.load_file(
    input_file=File( # entrada do arquivo
        path=S3_FILE_PATH + "/product_category.csv", conn_id=S3_CONN_ID
    ),
    output_table=Table( # saida em formato tabela no snowflake
        name=SNOWFLAKE_TABLE, conn_id=SNOWFLAKE_CONN_ID
        ),
    if_exists="replace"
    )
    
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            # "vars": '{"my_name": {{ params.my_name }} }',
            "models": MODELS_TO_RUN,  # os modelos vão rodar tanto run quanto test, o cosmos tem essa maravilha de função
        },
        default_args={"retries": 2},
    )


    extract_data >> transform_data 


