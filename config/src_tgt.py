import snowflake.connector
import pandas as pd
import yaml
from pathlib import Path

def source_csv():
    df = pd.read_csv(r"./data/customer.csv",nrows= 1009)
    return df


def snowflake_conn():

    config_path = Path(__file__).resolve().parent / "config.yaml"

    with open(config_path) as f:
        config = yaml.safe_load(f)

    conn = snowflake.connector.connect(
        user=config['snowflake']['user'],
        password=config['snowflake']['password'],
        account=config['snowflake']['account'],
        warehouse=config['snowflake']['warehouse'],
        database=config['snowflake']['database'],
        schema=config['snowflake']['schema'],
        role=config['snowflake']['role']
    )
    return conn
