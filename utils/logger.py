import logging
import os
from datetime import datetime

log_folder = "logs"
os.makedirs(log_folder, exist_ok=True)

log_file = os.path.join(
    log_folder,
    f"ETL_Validation_Report_{datetime.now().strftime('%d_%m_%Y_%H_%M_%S')}.log"
)

logger = logging.getLogger("ETL_REPORT")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(message)s")

file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


def log_test_result(test_name, start_time, end_time, status, error_msg=None):

    duration = (end_time - start_time).total_seconds()

    with open("etl_test_execution_log.txt", "a") as file:
        file.write(f"\nTest Case : {test_name}")
        file.write(f"\nStart Time: {start_time}")
        file.write(f"\nEnd Time  : {end_time}")
        file.write(f"\nDuration  : {duration} sec")
        file.write(f"\nStatus    : {status}")

        if error_msg:
            file.write(f"\nFailure Reason: {error_msg}")

        file.write("\n" + "="*50)

