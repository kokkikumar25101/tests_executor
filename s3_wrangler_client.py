import os
from line_utils_commons import logging
import awswrangler as wr
import boto3
import pandas as pd
from line_utils_commons.exception import LineDataServiceError
from line_utils_commons.constants import data_constants

logger = logging.getLogger(__name__)
BUCKET_NAME = os.path.expandvars('s3://$AWS_ACCOUNT_ID-$PARENT_STACKNAME-$ENVIRONMENT-datastore-bucket')
TABLE_BASE_PATH = '/tables'


AWS_ACCESS_KEY_ID = os.environ['SERVICE_EXECUTION_USER_ACCESSKEY']
AWS_SECRET_ACCESS_KEY = os.environ['SERVICE_EXECUTION_USER_SECRET']
AWS_REGION_ID = os.environ['AWS_REGION_ID']

boto_session = boto3.Session(
    region_name=AWS_REGION_ID,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def delete_files(file_path_prefix=['/tables/test']):
    logger.debug(f'Delete Partition Data ...file_path_prefix {file_path_prefix}')
    delete_path = f"{BUCKET_NAME}/{file_path_prefix}"
    wr.s3.delete_objects(delete_path, boto3_session=boto_session)
    logger.debug(f'Delete File from path {file_path_prefix}')


def write_dataframe_to_file(data_frame, file_path):
    logger.debug(f'write_dataframe_to_file {file_path}')
    _write_pandas_parquet_file_to_s3(data_frame, file_path=file_path)


def write_dataframe_to_partition(database_name, table_name, data_frame, partition_cols, mode=data_constants.S3_TABLE_PARTITION_MODE_OVERWRITE_PARTITION):
    _write_pandas_parquet_partition_to_s3(
        database_name,
        table_name,
        data_frame,
        mode,
        table_path=f'{TABLE_BASE_PATH}/{table_name}/',
        partition_cols=partition_cols
    )


def read_pandas_parquet_from_file_s3_filepaths(file_paths=['/tables/test']):
    logger.debug(f'Reading From S3 Starting ...file_paths {file_paths}')
    file_paths = [f's3://{BUCKET_NAME}/{key}' for key in file_paths]
    try:
        data_df = wr.s3.read_parquet(
            path=file_paths,
            boto3_session=boto_session
        )
        logger.debug(f'Returning {len(data_df)} Records ...')
        return data_df
    except Exception as e:
        # raise LineDataServiceError(e) from e
        logger.error(f"Error read_pandas_parquet_from_file_s3_filepaths {e}")
        return pd.DataFrame()


def read_pandas_parquet_from_file_s3_prefix(prefix_path='files/test'):
    logger.debug(f'Reading From S3 Starting ... {BUCKET_NAME}{prefix_path}')
    logger.debug(f'Reading From S3 Starting ... {BUCKET_NAME}{prefix_path}')
    try:
        data_df = wr.s3.read_parquet(
            path=f"{BUCKET_NAME}/{prefix_path}",
            boto3_session=boto_session
        )
        logger.debug(f'Returning {len(data_df)} Records ...')
        return data_df
    except Exception as e:
        # raise LineDataServiceError(e) from e
        logger.error(f"Error read_pandas_parquet_from_file_s3_prefix {e}")
        return pd.DataFrame()


def _write_pandas_parquet_file_to_s3(data_df, file_path='files/test'):
    # bucket_name = os.environ
    write_path = f"{BUCKET_NAME}/{file_path}"
    logger.debug(f'Write to S3 FileSystem Starting ... write_path {write_path}')
    try:
        wr.s3.to_parquet(
            df=data_df,
            path=write_path,
            # dataset=True,
            use_threads=True,
            compression='snappy',
            # mode='overwrite_partitions',
            boto3_session=boto_session
        )
    except Exception as e:
        logger.error(f'Error Writing to file {e}')
        raise LineDataServiceError(e) from e
    logger.debug(f'Successfully written file {len(data_df)} Records ...')


def _write_pandas_parquet_partition_to_s3(
    database_name,
    table_name,
    data_df,
    mode,
    table_path='/tables/test',
    partition_cols=[],
):
    # bucket_name = os.environ
    logger.debug('Write to S3 to Partition Starting ...')

    try:
        wr.s3.to_parquet(
            df=data_df,
            path=f"{BUCKET_NAME}{table_path}",
            dataset=True,
            partition_cols=partition_cols,
            use_threads=True,
            compression='snappy',
            database=database_name,  # Athena/Glue database
            table=table_name,
            concurrent_partitioning=True,
            mode=mode,
            boto3_session=boto_session
        )
    except Exception as e:
        raise LineDataServiceError(e) from e
    logger.debug(f'Successfully written to Partition {len(data_df)} Records ...')


def read_pandas_parquet_from_partition_s3(table_path='tables/test'):
    logger.debug('Reading From S3 Starting ...')
    try:
        data_df = wr.s3.read_parquet(
            path=f"{BUCKET_NAME}/{table_path}",
            dataset=True,
            # columns=['symbol', 'open'],and x['symbol'] == 'FUTIDX_BANKNIFTY_29OCT2020_XX_0'
            partition_filter=lambda x: True if x['exchange'] == 'NFO' and x['timeframe'] == '15M' and x['business_day'] == '2020-10-12'  else False,
            use_threads=True,
            boto3_session=boto_session
        )
        logger.debug(f'Returning {len(data_df)} Records ...')
        return data_df
    except Exception as e:
        # raise LineDataServiceError(e) from e
        logger.error(f"Error read_pandas_parquet_from_partition_s3 {e}")
        return pd.DataFrame()
