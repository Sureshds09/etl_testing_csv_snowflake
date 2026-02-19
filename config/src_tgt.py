import snowflake.connector
import pandas as pd
import os


def source_csv():
    df = pd.read_csv(r"./data/customer.csv", nrows=1009)
    return df


def snowflake_conn():

    user = os.getenv('SF_USER')
    password = os.getenv('SF_PASSWORD')
    account = os.getenv('SF_ACCOUNT')
    warehouse = os.getenv('SF_WAREHOUSE')
    database = os.getenv('SF_DATABASE')
    schema = os.getenv('SF_SCHEMA')

    if not user:
        raise Exception("SF_USER not found. GitHub Secrets not injected!")

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    return conn
