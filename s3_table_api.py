import os

import numpy as np
import pandas as pd

from line_data_core.s3.s3_table_utils import write_data_dataframe_to_file, read_pandas_from_file_s3_prefix
# from line_utils_commons.constants.data_constants import OHLC_COL_DATATYPE_MAPPING
from line_data_core.s3.s3_wrangler_client import \
    write_dataframe_to_partition
from line_utils_commons import logging
from line_utils_commons.api.dates_api import get_business_days_range
from line_utils_commons.constants import data_constants
from line_utils_commons.enums.data import OhlcEnum
from line_utils_commons.exception import LineDataServiceError

logger = logging.getLogger(__name__)

BUCKET_NAME = os.path.expandvars('s3://$AWS_ACCOUNT_ID-$PARENT_STACKNAME-$ENVIRONMENT-datastore-bucket')
ATHENA_DATABASE_NAME = os.path.expandvars('$PARENT_STACKNAME').replace('-', '')

PARTITION_COLS = ['exchange', 'timeframe', 'business_day']

# TABLE_BASE_PATH = f'/tables/{OHLC_TABLE_NAME}/'
FILES_BASE_PREFIX = f'files'
TABLES_BASE_PREFIX = f'tables'


# def write_ohlc_to_s3(table_name, ohlc_data, exchange, timeframe, business_day, mode=data_constants.S3_TABLE_PARTITION_MODE_OVERWRITE_PARTITION):
#     write_table_data_to_s3(
#         table_name,
#         ohlc_data,
#         exchange,
#         timeframe,
#         business_day,
#         mode=mode,
#         data_type_mapping=OhlcEnum.DATATYPE_MAPPING.value
#     )
#
#
# def write_lines_to_s3(table_name, ohlc_data, exchange, timeframe, business_day, mode=data_constants.S3_TABLE_PARTITION_MODE_OVERWRITE_PARTITION):
#     write_table_data_to_s3(
#         table_name,
#         ohlc_data,
#         exchange,
#         timeframe,
#         business_day,
#         mode=mode,
#         data_type_mapping=OhlcEnum.DATATYPE_MAPPING.value
#     )
#
#
# def write_strategies_to_s3(table_name, ohlc_data, exchange, timeframe, business_day, mode=data_constants.S3_TABLE_PARTITION_MODE_OVERWRITE_PARTITION):
#     write_table_data_to_s3(
#         table_name,
#         ohlc_data,
#         exchange,
#         timeframe,
#         business_day,
#         mode=mode,
#         data_type_mapping=OhlcEnum.DATATYPE_MAPPING.value
#     )


def write_table_data_to_s3(table_name, data_dict, exchange, timeframe, business_day, mode=data_constants.S3_TABLE_PARTITION_MODE_OVERWRITE_PARTITION, data_type_mapping=OhlcEnum.DATATYPE_MAPPING.value):
    if data_dict:
        # columns = ohlc_data[0].keys()
        # column_types = [OhlcEnum.DATATYPE_MAPPING[column] for column in columns]
        columns_dict = {}
        for column in data_dict[0].keys():
            if column in data_type_mapping:
                columns_dict[column] = pd.Series([], dtype=data_type_mapping[column])
        # ohlc_data_frame = pd.DataFrame(columns=columns, dtype=column_types)
        ohlc_data_frame = pd.DataFrame(columns_dict)
        ohlc_data_frame = ohlc_data_frame.append(data_dict, ignore_index=True)
        write_dataframe_to_s3(table_name, ohlc_data_frame, exchange, timeframe, business_day, mode)


def write_dataframe_to_s3(
    table_name,
    table_data_frame,
    exchange,
    timeframe,
    business_day,
    mode=data_constants.S3_TABLE_PARTITION_MODE_OVERWRITE_PARTITION
):
    write_data_dataframe_to_file(table_name, table_data_frame, exchange, timeframe, business_day)
    write_dataframe_to_partition(
        ATHENA_DATABASE_NAME,
        table_name,
        table_data_frame,
        PARTITION_COLS,
        mode
    )


def write_data_to_partition(table_name, data_dict):
    data_frame = pd.DataFrame(data_dict)
    write_dataframe_to_partition(
        ATHENA_DATABASE_NAME,
        table_name,
        data_frame,
        PARTITION_COLS
    )


def write_data_to_athena(table_name, data_frame):
    write_dataframe_to_partition(
        ATHENA_DATABASE_NAME,
        table_name,
        data_frame,
        PARTITION_COLS
    )


def write_data_to_file(table_name, data_dict, exchange, timeframe, business_day, run_identifier='default'):
    data_frame = pd.DataFrame(data_dict)
    file_path = write_data_dataframe_to_file(table_name, data_frame, exchange, timeframe, business_day, run_identifier)
    return file_path


# def write_ohlc_dataframe_to_file(table_name, data_frame, exchange, timeframe, business_day, run_identifier='default'):
#     logger.debug(f'Write OHLC to File '
#           f'exchange {exchange}, '
#           f'timeframe {timeframe}, '
#           f'business_day {business_day}, '
#           f'run_identifier {run_identifier}'
#      )
#     current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
#     file_path = f'files/{table_name}/{exchange}/{timeframe}/{business_day}/{run_identifier}/{current_timestamp}.parquet'
#     logger.debug(f'Writing to Path {file_path}')
#     write_dataframe_to_file(data_frame, file_path=file_path)
#     return file_path


def write_data_symbol_split_to_file(data_records, exchange, timeframe, business_day):
    for data_record in data_records:
        write_data_to_file(data_record['records'], exchange, timeframe, business_day, data_record['symbol'])
#
#
# def read_pandas_from_file_s3_prefix(prefix_path='/files/test'):
#     result_dataframe = read_pandas_parquet_from_file_s3_prefix(prefix_path)
#     return result_dataframe
#
#
# def read_pandas_from_run_identifier_files(table_name, exchange, timeframe, business_day, run_identifier):
#     prefix_path = f'{FILES_BASE_PREFIX}/{table_name}/{exchange}/{timeframe}/{business_day}/{run_identifier}/'
#     result_dataframe = read_pandas_from_file_s3_prefix(prefix_path=prefix_path)
#     return result_dataframe
#
#
# def list_ohlc_objects(table_name, exchange, timeframe, business_day, run_identifier):
#     prefix_path = f'{FILES_BASE_PREFIX}/{table_name}/{exchange}/{timeframe}/{business_day}/{run_identifier}/'
#     file_paths = list_objects(prefix_path)
#     return file_paths
#
#
# def delete_partition_data(table_name, exchange, timeframe, business_day):
#     prefix_path = f'{TABLES_BASE_PREFIX}/{table_name}/exchange={exchange}/timeframe={timeframe}/business_day={business_day}/'
#     delete_files(file_path_prefix=prefix_path)


def populate_data_to_tables_from_files(exchange, timeframe, from_date, to_date, table_name='lines'):
    dates = get_business_days_range(from_date, to_date)
    # dates = ['2021-01-05']
    for business_day in dates:
        logger.info(business_day)
        prefix_path = f'{FILES_BASE_PREFIX}/{table_name}/{exchange}/{timeframe}/{business_day}/default/'
        try:
            df_read = read_pandas_from_file_s3_prefix(prefix_path=prefix_path)
            logger.info(len(df_read))

            # df_read_write = pd.DataFrame(df_read["cl_ind,prev_cl_ind"].to_list(), columns=['cl_ind', 'prev_cl_ind'])
            # copy_df = df_read.copy()
            # copy_df["Antecedent,Consequent"] = df_read["Antecedent,Consequent"].apply(list)
            # table = pa.Table.from_pandas(copy_df).to_pandas()
            # st.dataframe(table)
            write_data_to_partition(table_name, df_read)

        except LineDataServiceError as line_data_sevice_error:
            logger.error(f'{business_day} Error : {line_data_sevice_error}')


def populate_ohlc_to_tables_from_files(exchange, timeframe, from_date, to_date, table_name='ohlc'):
    pass
    dates = get_business_days_range(from_date, to_date)
    # dates = ['2021-01-05']
    for business_day in dates:
        logger.info(business_day)
        prefix_path = f'{FILES_BASE_PREFIX}/{table_name}/{exchange}/{timeframe}/{business_day}/default/'
        try:
            df_read = read_pandas_from_file_s3_prefix(prefix_path=prefix_path)
            logger.info(len(df_read))

            df_read["lasttradetime"] = np.where(df_read["lasttradetime"] == 'None', "0", df_read["lasttradetime"])
            df_read["openinterest"] = np.where(df_read["openinterest"] == 'None', "0", df_read["lasttradetime"])
            df_read["quotationlot"] = np.where(df_read["quotationlot"] == 'None', "0", df_read["lasttradetime"])
            df_read["tradedqty"] = np.where(df_read["tradedqty"] == 'None', "0", df_read["lasttradetime"])

            write_data_to_partition(table_name, df_read)

        except LineDataServiceError as line_data_sevice_error:
            logger.error(f'{business_day} Error : {line_data_sevice_error}')


if __name__ == '__main__':
    # pass
# #     # timeframe = '15M'
    exchange = 'NFO'
    from_date = "2023-12-21"
    to_date = "2023-12-21"
    timeframe = '15M'
#     # dates = get_business_day_range(from_date, to_date)
#     # print(dates)
#     populate_ohlc_to_tables_from_files(exchange, timeframe, from_date, to_date)

    populate_data_to_tables_from_files(exchange, timeframe, from_date, to_date, table_name='strategy')