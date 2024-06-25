
from line_data_core.mysql.legacy_connector import load_ohlc_data
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import tempfile
import s3fs
from line_utils_commons import logging


logger = logging.getLogger(__name__)
s3 = s3fs.S3FileSystem()
s3_client = boto3.client("s3")


def write_pandas_parquet_to_s3_pyarrow(data_df, bucket_name, key_name, file_name):
    # dummy dataframe
    table = pa.Table.from_pandas(data_df)
    # pq.write_table(table, file_name)

    pq.write_to_dataset(
        table,
        bucket_name,
        partition_cols=['exchange', 'timeframe', 'businessDay', 'symbol'],
        use_legacy_dataset=False,
        filesystem=s3
    )
    # upload to s3
    # buf = pa.BufferOutputStream()
    #
    # with open(file_name, encoding='utf-8') as f:
    #     object_data = f.read()
    #     s3_client.put_object(Body=object_data, Bucket=bucket_name, Key=key_name)


if __name__ == '__main__':

    timeframe = '15M'
    exchange = 'NFO'
    from_date = "2020-10-15"
    to_date = "2020-10-15"

    results = load_ohlc_data(from_date, to_date, exchange, target='pandas')
    df = pd.DataFrame(results)


    file_name = f"{exchange}-{timeframe}-{from_date}.parquet"
    s3_bucket = 's3://270058833930-line-v1-datastore-bucket/tables/ohlc/'

    logger.debug(f"{tempfile.gettempdir()}/{file_name}")
    write_pandas_parquet_to_s3(
        df, s3_bucket, f"{exchange}/{timeframe}/{from_date}/{file_name}", f"{tempfile.gettempdir()}/{file_name}")
    # results = load_ohlc_data(from_date, to_date, 'NSE', 'mysql')
    logger.debug(df.iloc[0])
    logger.debug(len(df))

    # dal.save_ohlc(results[0])
    # dal.save_ohlc_batch(results)