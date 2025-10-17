from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "export_to_postgres",
    default_args=default_args,
    description="Export curated data from Iceberg to PostgreSQL",
    schedule=timedelta(days=1),
    catchup=False,
)

# Create PostgreSQL tables
create_dim_product = PostgresOperator(
    task_id="create_dim_product_table",
    postgres_conn_id="postgres_serving",
    sql="""
    CREATE TABLE IF NOT EXISTS dim_product (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(255),
        product_category VARCHAR(255),
        product_subcategory VARCHAR(255),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    TRUNCATE TABLE dim_product;
    """,
    dag=dag,
)

create_fact_sale = PostgresOperator(
    task_id="create_fact_sale_table",
    postgres_conn_id="postgres_serving",
    sql="""
    CREATE TABLE IF NOT EXISTS fact_sale (
        sale_id INTEGER PRIMARY KEY,
        product_id INTEGER,
        territory_id INTEGER,
        sale_date DATE,
        quantity INTEGER,
        revenue DECIMAL(15,2),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
    );
    TRUNCATE TABLE fact_sale;
    """,
    dag=dag,
)


def export_table_to_postgres(source_table, target_table, **context):
    """Export data from Trino/Iceberg to PostgreSQL"""

    # Trino connection
    trino_engine = create_engine(
        "trino://trino-coordinator:8080/iceberg/curated",
        connect_args={"auth": None, "http_scheme": "http"},
    )

    # PostgreSQL connection using Airflow connection
    postgres_hook = PostgresHook(postgres_conn_id="postgres_serving")
    pg_engine = postgres_hook.get_sqlalchemy_engine()

    # Read from Trino
    query = f"SELECT * FROM {source_table}"
    df = pd.read_sql(query, trino_engine)

    # Write to PostgreSQL
    df.to_sql(
        target_table,
        pg_engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    print(f"Exported {len(df)} rows from {source_table} to {target_table}")


export_dim_product = PythonOperator(
    task_id="export_dim_product",
    python_callable=export_table_to_postgres,
    op_kwargs={"source_table": "dim_product", "target_table": "dim_product"},
    dag=dag,
)

export_fact_sale = PythonOperator(
    task_id="export_fact_sale",
    python_callable=export_table_to_postgres,
    op_kwargs={"source_table": "fact_sale", "target_table": "fact_sale"},
    dag=dag,
)

# Dependencies
create_dim_product >> export_dim_product
create_fact_sale >> export_fact_sale
