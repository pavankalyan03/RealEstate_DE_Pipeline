from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from include.scripts.functions import clean_json, upload_into_stage

DATABRICKS_NOTEBOOK_PATH_1 = f"/Users/pavan.kotha@anblicks.com/Real Estate DE Project/BRONZE Real Estate"
DATABRICKS_NOTEBOOK_PATH_2 = f"/Users/pavan.kotha@anblicks.com/Real Estate DE Project/SILVER Real Estate"
DATABRICKS_CONN_ID = "databricks_default"
DATABRICKS_EXISTING_CLUSTER_ID = "0612-203655-qiw2wsxb"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  
    'start_date': datetime(2023, 10, 1),
    'retries': 0
    }

with DAG (
    dag_id = 'ETL_pipeline_remastered',
    default_args=default_args,
    catchup=False,

    # schedule_interval='@daily',  
) as dag:
    
    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_json,
        provide_context=True
    )

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_into_stage,
        provide_context=True
    )

    call_bronze_notebook = DatabricksNotebookOperator(
        task_id="run_notebook_1",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_path=DATABRICKS_NOTEBOOK_PATH_1,
        source="WORKSPACE",
        existing_cluster_id=DATABRICKS_EXISTING_CLUSTER_ID,
    )

    call_silver_notebook = DatabricksNotebookOperator(
        task_id="run_notebook_2",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_path=DATABRICKS_NOTEBOOK_PATH_2,
        source="WORKSPACE",
        existing_cluster_id=DATABRICKS_EXISTING_CLUSTER_ID,
    )

    ingest_into_dimensions = SQLExecuteQueryOperator(
        task_id="ingest_into_dimensions",
        conn_id= 'snowflake_default',
        database="REALESTATE",
        sql="CALL REALESTATE.GOLD.populate_real_estate_dimensions();",
        autocommit=True
    )

    ingest_into_facts = SQLExecuteQueryOperator(
        task_id="ingest_into_facts",
        conn_id='snowflake_default',
        database="REALESTATE",
        sql="CALL REALESTATE.GOLD.populate_fact_property_listings();",
        autocommit=True

    )

    ingest_into_nearby_places = SQLExecuteQueryOperator(
        task_id="ingest_into_nearby_places",
        conn_id='snowflake_default',
        database="REALESTATE",
        sql="CALL REALESTATE.GOLD.populate_fact_property_nearby_places();",
        autocommit=True

    )

    clean_task >> upload_task >> call_bronze_notebook >> call_silver_notebook >> ingest_into_dimensions >> ingest_into_facts >> ingest_into_nearby_places   