import os
from datetime import datetime
from cosmos.airflow.dag import DbtDag
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles.snowflake import SnowflakeUserPasswordProfileMapping


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snow_connection", 
        profile_args={
            "account": "NMNTUTW-OX01383",
            "user": "C4LEB",
            "role": "DBT_ROLE",
            "password": "closed$50K2025",
            "warehouse": "DBT_WH",
            "database": "DBT_DB",
            "schema": "DBT_SCHEMA"
            },
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt_pipeline",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
)
