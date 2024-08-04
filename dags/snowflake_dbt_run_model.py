from airflow.models import DAG
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

from pendulum import datetime
import os

VARIABLE = "var_value"

# O caminho para o projeto dbt
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt-dw-etl/project-etl"
# O caminho onde o Cosmos encontrará o executável do dbt
# no ambiente virtual criado no Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt" 
# mudar a depender da necessidade e teste f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/activate"

MODEL_TO_QUERY = "stg_product_category" # 1 modelo
MODELS_TO_RUN = "stg_product_category, tru_product_category" # 2 ou mais

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
    dag_id = "snowflake_dbt_run",
    start_date = datetime(2024, 8, 4),
    schedule = None,
    catchup = False,
    # params={"VARIAVEL": VAR_DEFINED}, # Utilizamos em caso de passar a variavel diretamente pelo airflow no trigger
    tags = ["snowflake","etl"]
    )

with dag:    
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


    transform_data 

