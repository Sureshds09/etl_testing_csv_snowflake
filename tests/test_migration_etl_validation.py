import pytest
from datetime import datetime
from utils.logger import logger
import pandas as pd


# ================= SUITE TIMER =================
@pytest.fixture(scope="session", autouse=True)
def execution_timer():

    suite_start = datetime.now()
    logger.info("=================================================")
    logger.info(f"ETL VALIDATION STARTED AT : {suite_start.strftime('%d-%m-%Y %H:%M:%S')}")
    logger.info("=================================================")

    yield

    suite_end = datetime.now()
    logger.info("=================================================")
    logger.info(f"ETL VALIDATION ENDED AT : {suite_end.strftime('%d-%m-%Y %H:%M:%S')}")
    logger.info(f"TOTAL EXECUTION TIME : {suite_end - suite_start}")
    logger.info("=================================================")


# ================= FILE LOAD =================
@pytest.mark.file_check
def test_file_loaded(src):

    logger.info("******** FILE IDENTIFICATION VALIDATION ********")
    assert not src.empty
    logger.info(f"SOURCE FILE RECORD COUNT : {len(src)}\n")


# ================= SCHEMA =================
@pytest.mark.schema_val
def test_schema_validation(src, tgt):

    logger.info("******** SCHEMA VALIDATION ********")
    assert set(src.columns) == set(tgt.columns)
    logger.info("SCHEMA MATCHED\n")


# ================= COLUMN COUNT =================
@pytest.mark.count_of_column_val
def test_column_count(src, tgt):

    logger.info("******** COLUMN COUNT VALIDATION ********")
    assert src.shape[1] == tgt.shape[1]
    logger.info("COLUMN COUNT MATCHED\n")


# ================= ROW COUNT =================
@pytest.mark.row_count_val
def test_row_count(src, tgt):

    logger.info("******** ROW COUNT VALIDATION ********")
    assert len(src) == len(tgt)
    logger.info("ROW COUNT MATCHED\n")


# ================= NULL =================
@pytest.mark.null_count_val
def test_null_validation(src, tgt):

    logger.info("******** NULL VALIDATION ********")

    src_null = src.isnull().sum().sum()
    tgt_null = tgt.isnull().sum().sum()

    logger.info(f"SRC NULL : {src_null}")
    logger.info(f"TGT NULL : {tgt_null}")

    assert src_null == tgt_null
    logger.info("NULL VALIDATION PASSED\n")


# ================= DUPLICATE =================
@pytest.mark.duplicate_count_val
def test_duplicate_validation(src, tgt):

    logger.info("******** DUPLICATE VALIDATION ********")

    src_dup = src.duplicated().sum()
    tgt_dup = tgt.duplicated().sum()

    logger.info(f"SRC DUP : {src_dup}")
    logger.info(f"TGT DUP : {tgt_dup}")

    assert src_dup == tgt_dup
    logger.info("DUPLICATE VALIDATION PASSED\n")


# ================= PRIMARY KEY =================
@pytest.mark.PK_val
def test_primary_key(src, tgt):

    logger.info("******** PRIMARY KEY VALIDATION ********")

    assert src['C_CUSTKEY'].is_unique
    assert tgt['C_CUSTKEY'].is_unique

    logger.info("PRIMARY KEY UNIQUENESS PASSED\n")


# ================= DATATYPE =================
@pytest.mark.data_type_val
def test_datatype_validation(src, tgt):

    logger.info("******** DATA TYPE VALIDATION ********")

    src_dtype = src.dtypes.to_dict()
    tgt_dtype = tgt.dtypes.to_dict()

    assert src_dtype.keys() == tgt_dtype.keys()
    logger.info("DATA TYPE VALIDATION PASSED\n")


# ================= RANGE =================
@pytest.mark.range_val
def test_range_validation(src, tgt):

    logger.info("******** RANGE VALIDATION ********")

    assert src['C_CUSTKEY'].min() == tgt['C_CUSTKEY'].min()
    assert src['C_CUSTKEY'].max() == tgt['C_CUSTKEY'].max()

    logger.info("MIN/MAX RANGE MATCHED\n")


# ================= UNIQUE =================
@pytest.mark.unique_key_val
def test_unique_validation(src, tgt):

    logger.info("******** UNIQUE VALUE VALIDATION ********")

    assert src['C_CUSTKEY'].nunique() == tgt['C_CUSTKEY'].nunique()
    logger.info("UNIQUE VALUE MATCHED\n")


# ================= NOT NULL =================
@pytest.mark.not_null_constraint_val
def test_not_null_constraint(src, tgt):

    logger.info("******** NOT NULL CONSTRAINT ********")

    assert src['C_CUSTKEY'].isnull().sum() == 0
    assert tgt['C_CUSTKEY'].isnull().sum() == 0

    logger.info("NOT NULL CONSTRAINT PASSED\n")


# ================= AGGREGATE =================
@pytest.mark.aggregation_val
def test_aggregate_validation(src, tgt):

    logger.info("******** AGGREGATE VALIDATION ********")

    assert src['C_CUSTKEY'].sum() == tgt['C_CUSTKEY'].sum()
    logger.info("SUM MATCHED\n")


# ================= LENGTH =================
@pytest.mark.row_cnt_val
def test_length_validation(src, tgt):

    logger.info("******** LENGTH VALIDATION ********")

    src_len = src['C_NAME'].str.len().sum()
    tgt_len = tgt['C_NAME'].str.len().sum()

    assert src_len == tgt_len
    logger.info("STRING LENGTH MATCHED\n")


# ================= FORMAT =================
@pytest.mark.data_format_val
def test_format_validation(src):

    logger.info("******** FORMAT VALIDATION ********")

    assert src['C_CUSTKEY'].astype(str).str.isnumeric().all()
    logger.info("FORMAT VALIDATION PASSED\n")


# ================= BUSINESS RULE =================
@pytest.mark.business_rule_val
def test_business_rule(src):

    logger.info("******** BUSINESS RULE VALIDATION ********")

    assert (src['C_CUSTKEY'] > 0).all()
    logger.info("BUSINESS RULE PASSED\n")


# ================= MISSING =================
# ================= MISSING =================
@pytest.mark.missing_tgt_val
def test_missing_in_target(request, src, tgt):

    logger.info("******** MISSING RECORD VALIDATION ********")

    missing = src[~src['C_CUSTKEY'].isin(tgt['C_CUSTKEY'])]

    logger.info(f"MISSING COUNT : {missing.shape[0]}")

    request.node.failed_records = missing
    request.node.fail_type = "Missing in Target"

    assert missing.shape[0] == 0


# ================= EXTRA =================
@pytest.mark.extra_in_tgt_val
def test_extra_in_target(request, src, tgt):

    logger.info("******** EXTRA RECORD VALIDATION ********")

    extra = tgt[~tgt['C_CUSTKEY'].isin(src['C_CUSTKEY'])]

    logger.info(f"EXTRA COUNT : {extra.shape[0]}")

    request.node.failed_records = extra
    request.node.fail_type = "Extra in Target"

    assert extra.shape[0] == 0



# ================= DATA MATCH =================

@pytest.mark.data_match_val
def test_data_comparison(request, src, tgt):

    logger.info("******** DATA COMPARISON ********")

    # Work only on Business Key
    src['C_CUSTKEY'] = pd.to_numeric(src['C_CUSTKEY'], errors='coerce').fillna(-999)
    tgt['C_CUSTKEY'] = pd.to_numeric(tgt['C_CUSTKEY'], errors='coerce').fillna(-999)

    diff = src.merge(
        tgt,
        on='C_CUSTKEY',
        how='outer',
        indicator=True
    ).query("_merge!='both'")

    logger.info(f"MISMATCH COUNT : {diff.shape[0]}")

    if diff.shape[0] != 0:

        request.node.failed_records = diff
        request.node.fail_type = "Data Mismatch"

        pytest.fail("Data Mismatch Found")