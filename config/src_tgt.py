import snowflake.connector
import pandas as pd
import os
import snowflake.connector

def source_csv():
    df = pd.read_csv(r"./data/customer.csv",nrows= 1009)
    return df



def snowflake_conn():

    conn = snowflake.connector.connect(
        user=os.environ['SF_USER'],
        password=os.environ['SF_PASSWORD'],
        account=os.environ['SF_ACCOUNT'],
        warehouse=os.environ['SF_WAREHOUSE'],
        database=os.environ['SF_DATABASE'],
        schema=os.environ['SF_SCHEMA']
    )

    return conn


