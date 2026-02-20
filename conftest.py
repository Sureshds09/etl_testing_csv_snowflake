

import pytest
import pandas as pd
from config.src_tgt import source_csv, snowflake_conn
from datetime import datetime
from utils.logger import log_test_result
from utils.excel_report import write_defect_to_excel
from utils.logger import logger


# ================= SOURCE FIXTURE =================
@pytest.fixture(scope="session")
def src():

    print("\nConnecting to Source CSV...")
    df = source_csv()

    # Match Snowflake column naming
    df.columns = df.columns.str.upper()
    df['C_CUSTKEY'] = pd.to_numeric(df['C_CUSTKEY'], errors='coerce')
    df['C_NATIONKEY'] = pd.to_numeric(df['C_NATIONKEY'], errors='coerce')



    # Sort using Business Key
    df = df.sort_values(by='C_CUSTKEY')

    yield df

    print("\nSource CSV connection closed.")


# ================= TARGET FIXTURE =================
@pytest.fixture(scope="session")
def tgt():

    print("\nConnecting to Snowflake Target...")
    conn = snowflake_conn()

    df = pd.read_sql("""
    SELECT *
    FROM CUSTOMER
    ORDER BY C_CUSTKEY ASC
    LIMIT 1000
    """, conn)

    df.columns = df.columns.str.upper()
    df['C_CUSTKEY'] = pd.to_numeric(df['C_CUSTKEY'], errors='coerce')
    df['C_NATIONKEY'] = pd.to_numeric(df['C_NATIONKEY'], errors='coerce')




    yield df

    conn.close()
    print("\nSnowflake connection closed.")




@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):

    outcome = yield
    rep = outcome.get_result()

    if rep.when == "call" and rep.failed:

        failed_data = getattr(item, "failed_records", None)
        fail_type = getattr(item, "fail_type", "Validation Failed")

        if failed_data is not None and not failed_data.empty:

            logger.error(f"\nTEST FAILED : {item.name}")
            logger.error(f"FAIL TYPE : {fail_type}")

            write_defect_to_excel(item.name, fail_type, failed_data)



