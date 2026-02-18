import pandas as pd
from datetime import datetime
from openpyxl import load_workbook
import os

def write_defect_to_excel(test_name, fail_type, failed_data):

    base_path = os.getcwd()

    # Excel will be created here
    file_name = os.path.join(base_path, "reports", "etl_defect_report.xlsx")

    # Create reports folder automatically
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    sheet = test_name[:30]

    failed_data['FAIL_TYPE'] = fail_type
    failed_data['TEST_CASE'] = test_name
    failed_data['EXECUTION_TIME'] = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    if not os.path.exists(file_name):

        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            failed_data.to_excel(writer, sheet_name=sheet, index=False)

    else:

        with pd.ExcelWriter(file_name, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            failed_data.to_excel(writer, sheet_name=sheet, index=False)
            
            print(file_name)


