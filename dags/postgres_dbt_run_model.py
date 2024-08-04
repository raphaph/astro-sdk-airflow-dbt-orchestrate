from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime
import os

CONNECTION_ID = "postgres_default"
DB_NAME = "postgres"
SCHEMA_NAME = "public"
MODELS_TO_RUN = "model1,model2"

# O caminho para o projeto dbt
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt-dw-etl/project-etl"
# O caminho onde o Cosmos encontrará o executável do dbt
# no ambiente virtual criado no Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt" 
# alternativa f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/activate"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

dag = DAG(
    dag_id = "postgres_dbt_run",
    start_date = datetime(2024, 8, 4),
    schedule = None,
    catchup = False,
    tags = ["postgres","etl"]
)
with dag:
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "models": MODELS_TO_RUN,  # Especifica o modelo dbt a ser rodado
        },
        default_args={"retries": 2},
    )

    transform_data 