import csv
import logging

from pydomo import Domo, Column, ColumnType, Schema
from pydomo import DataSetRequest

from commons import log_csv
from config.env import CLIENT_ID, CLIENT_SECRET, LOG_DATASET_ID
from utils import send_email_error
from logger_config import setup_logging

# Setup logging
setup_logging(module_name=" Upload Log data")
logger = logging.getLogger(__name__)


def get_new_column(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)


date_time_Log = {'Date'}


def get_column_type(column_name, date_time_columns):
    if column_name in date_time_columns:
        return ColumnType.DATETIME
    else:
        return ColumnType.STRING


def upload_dataset(domo, dataset_id, file_path, date_time_columns,method="REPLACE"):
    # Update a DataSet's metadata
    ds = domo.datasets
    update = DataSetRequest()
    # update.name = os.path.basename(file_path)

    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)

    update.schema = Schema([Column(get_column_type(col, date_time_columns), col) for col in headers])

    ds.update(dataset_id, update)
    ds.data_import_from_file(dataset_id, file_path,update_method=method)
    logger.info("Uploaded data from a file to Dataset {}".format(dataset_id))


def upload_log_to_domo():
    try:
        datasets_info = [
            (LOG_DATASET_ID, log_csv, date_time_Log)
        ]
        domo = Domo(CLIENT_ID, CLIENT_SECRET, api_host='api.domo.com')

        for dataset_id, filepath, date_time_columns in datasets_info:
            upload_dataset(domo, dataset_id, filepath, date_time_columns, method='APPEND')
    except Exception as exc:
        send_email_error(exc)


if __name__ == "__main__":
    upload_log_to_domo()
