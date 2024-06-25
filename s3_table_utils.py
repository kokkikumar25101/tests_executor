import os
import pandas as pd
import numpy as np
from line_utils_commons import logging
from datetime import datetime
from line_utils_commons.clients.s3_client import list_objects
from line_utils_commons.enums.data import OhlcEnum
from line_utils_commons.constants import data_constants
# from line_utils_commons.constants.data_constants import OHLC_COL_DATATYPE_MAPPING
from line_data_core.s3.s3_wrangler_client import \
    write_dataframe_to_file, \
    read_pandas_parquet_from_file_s3_prefix, \
    write_dataframe_to_partition, delete_files

from line_utils_commons.exception import LineDataServiceError
from line_utils_commons.api.dates_api import get_business_days_range

logger = logging.getLogger(__name__)

BUCKET_NAME = os.path.expandvars('s3://$AWS_ACCOUNT_ID-$PARENT_STACKNAME-$ENVIRONMENT-datastore-bucket')
ATHENA_DATABASE_NAME = os.path.expandvars('$PARENT_STACKNAME').replace('-', '')

PARTITION_COLS = ['exchange', 'timeframe', 'business_day']

# TABLE_BASE_PATH = f'/tables/{OHLC_TABLE_NAME}/'
FILES_BASE_PREFIX = f'files'
TABLES_BASE_PREFIX = f'tables'


# def write_ohlc_to_s3(table_name, ohlc_data, exchange, timeframe, business_day, mode=data_constants.S3_TABLE_PARTITION_MODE_OVERWRITE_PARTITION):
#     if ohlc_data:
#         # columns = ohlc_data[0].keys()
#         # column_types = [OhlcEnum.DATATYPE_MAPPING[column] for column in columns]
#         columns_dict = {}
#         for column in ohlc_data[0].keys():
#             columns_dict[column] = pd.Series([], dtype=OhlcEnum.DATATYPE_MAPPING.value[column])
#         # ohlc_data_frame = pd.DataFrame(columns=columns, dtype=column_types)
#         ohlc_data_frame = pd.DataFrame(columns_dict)
#         ohlc_data_frame = ohlc_data_frame.append(ohlc_data, ignore_index=True)
#         write_ohlc_dataframe_to_s3(table_name, ohlc_data_frame, exchange, timeframe, business_day, mode)
#
#
# def write_ohlc_dataframe_to_s3(
#     table_name,
#     ohlc_data_frame,
#     exchange,
#     timeframe,
#     business_day,
#     mode=data_constants.S3_TABLE_PARTITION_MODE_OVERWRITE_PARTITION
# ):
#     write_ohlc_dataframe_to_file(table_name, ohlc_data_frame, exchange, timeframe, business_day)
#     write_dataframe_to_partition(
#         ATHENA_DATABASE_NAME,
#         table_name,
#         ohlc_data_frame,
#         PARTITION_COLS,
#         mode
#     )


def write_data_to_partition(table_name, data_dict):
    data_frame = pd.DataFrame(data_dict)
    write_dataframe_to_partition(
        ATHENA_DATABASE_NAME,
        table_name,
        data_frame,
        PARTITION_COLS
    )


def write_dataframe_to_athena(table_name, data_frame):
    write_dataframe_to_partition(
        ATHENA_DATABASE_NAME,
        table_name,
        data_frame,
        PARTITION_COLS
    )


def write_data_to_file(table_name, table_data, exchange, timeframe, business_day, run_identifier='default'):
    data_frame = pd.DataFrame(table_data)
    file_path = write_data_dataframe_to_file(table_name, data_frame, exchange, timeframe, business_day, run_identifier)
    return file_path


def write_data_dataframe_to_file(table_name, data_frame, exchange, timeframe, business_day, run_identifier='default'):
    logger.debug(f'Write DATA to File '
          f'exchange {exchange}, '
          f'timeframe {timeframe}, '
          f'business_day {business_day}, '
          f'run_identifier {run_identifier}'
     )
    current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    file_path = f'files/{table_name}/{exchange}/{timeframe}/{business_day}/{run_identifier}/{current_timestamp}.parquet'
    logger.debug(f'Writing to Path {file_path}')
    write_dataframe_to_file(data_frame, file_path=file_path)
    return file_path


# def write_ohlc_symbol_split_to_file(ohlc_records, exchange, timeframe, business_day):
#     for ohlc_record in ohlc_records:
#         write_ohlc_to_file(ohlc_record['records'], exchange, timeframe, business_day, ohlc_record['symbol'])


def read_pandas_from_file_s3_prefix(prefix_path='/files/test'):
    result_dataframe = read_pandas_parquet_from_file_s3_prefix(prefix_path)
    return result_dataframe


def read_pandas_from_run_identifier_files(table_name, exchange, timeframe, business_day, run_identifier):
    prefix_path = f'{FILES_BASE_PREFIX}/{table_name}/{exchange}/{timeframe}/{business_day}/{run_identifier}/'
    result_dataframe = read_pandas_from_file_s3_prefix(prefix_path=prefix_path)
    return result_dataframe


def list_files_objects(table_name, exchange, timeframe, business_day, run_identifier):
    prefix_path = f'{FILES_BASE_PREFIX}/{table_name}/{exchange}/{timeframe}/{business_day}/{run_identifier}/'
    file_paths = list_objects(prefix_path)
    return file_paths


def delete_partition_data(table_name, exchange, timeframe, business_day):
    prefix_path = f'{TABLES_BASE_PREFIX}/{table_name}/exchange={exchange}/timeframe={timeframe}/business_day={business_day}/'
    delete_files(file_path_prefix=prefix_path)


# def populate_ohlc_to_tables_from_files(exchange, timeframe, from_date, to_date, table_name='ohlc'):
#     pass
#     dates = get_business_days_range(from_date, to_date)
#     # dates = ['2021-01-05']
#     for business_day in dates:
#         logger.info(business_day)
#         prefix_path = f'{FILES_BASE_PREFIX}/{table_name}/{exchange}/{timeframe}/{business_day}/default/'
#         try:
#             df_read = read_pandas_from_file_s3_prefix(prefix_path=prefix_path)
#             logger.info(len(df_read))
#
#             df_read["lasttradetime"] = np.where(df_read["lasttradetime"] == 'None', "0", df_read["lasttradetime"])
#             df_read["openinterest"] = np.where(df_read["openinterest"] == 'None', "0", df_read["lasttradetime"])
#             df_read["quotationlot"] = np.where(df_read["quotationlot"] == 'None', "0", df_read["lasttradetime"])
#             df_read["tradedqty"] = np.where(df_read["tradedqty"] == 'None', "0", df_read["lasttradetime"])
#
#             write_ohlc_to_partition(table_name, df_read)
#
#         except LineDataServiceError as line_data_sevice_error:
#             logger.error(f'{business_day} Error : {line_data_sevice_error}')


# if __name__ == '__main__':
#     pass
# #     # timeframe = '15M'
#     exchange = 'NFO'
#     from_date = "2022-01-05"
#     to_date = "2022-01-05"
#     timeframe = '15M'
#     # dates = get_business_day_range(from_date, to_date)
#     # print(dates)
#     populate_ohlc_to_tables_from_files(exchange, timeframe, from_date, to_date)