import os
import snowflake.connector

conn = snowflake.connector.connect(
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    account=os.getenv("SF_ACCOUNT")
)

print("Snowflake Connected Successfully")
