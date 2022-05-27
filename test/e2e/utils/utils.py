import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime
import boto3
import warnings

warnings.filterwarnings("ignore")
session = boto3.Session()
credentials = session.get_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key


def get_spark():
    """
    Creating spark to be able to read data from S3 bucket
    :return:
    """
    spark = (
        SparkSession
            .builder
            .master("local[*]")
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2')
            .config("fs.s3a.access.key", access_key)
            .config("fs.s3a.secret.key", secret_key)
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config("fs.s3a.endpoint.key", "s3.amazonaws.com")
            .config("spark.executor.memory", "70g")
            .config("spark.driver.memory", "50g")
            .config("spark.memory.offHeap.enabled", True)
            .config("spark.memory.offHeap.size", "32g")
            .getOrCreate()
    )
    return spark


def compare_validate_schema(df_ms, df):
    """
    Validate schema by comparing mapping spec and Dataframe schema
    :param df_ms: Pandas DataFrame, expected schema
    :param df: Spark DataFrame
    :return:
    """
    all_columns = set()
    all_columns.update(df.columns)
    all_columns.update(df_ms['column_name'])
    all_columns = list(all_columns)

    df_result = pd.DataFrame(columns=['column_name', 'ms', 's3'])

    for i in range(len(all_columns)):
        col_name = all_columns[i]
        df_result = df_result.append({'column_name': col_name,
                                      'ms': (df_ms['data_type'][df_ms['column_name'] == col_name]).values[0],
                                      's3': str(df.schema[col_name].dataType)}, ignore_index=True)
    df_diff = df_result[df_result.isna().any(axis=1) | (df_result['ms'] != df_result['s3'])]
    print('========= The difference between MS and S3 =====')
    print(df_diff)
    assert len(df_diff) == 0, f'The MS and Data Schema does not match!!!'


def validate_pk(spark, df, pk_column_list):
    """
    Checks PK of dataframe
    :param spark: Spark
    :param df: Dataframe
    :param pk_column_list: PK column list of the Dataframe
    :return:
    """
    df.createOrReplaceTempView('table')
    pk_columns_str = ','.join(pk_column_list)
    query = f'select count(*) as count from table group by {pk_columns_str} having count(*) > 1'
    # df.groupBy(*pk_column_list).count().where(col('count') > 1) >> same function different way
    result = spark.sql(query)
    duplicated_rows = result.groupby().sum().collect()[0][0]
    assert duplicated_rows == None, f'{duplicated_rows} duplication found!!!'


def validate_not_null_columns(spark, df, not_null_column_list):
    """
    Checks not null for columns
    :param spark: Spark
    :param df: DataFrame
    :param not_null_column_list: list of not-null columns
    :return:
    """
    df.createOrReplaceTempView('table')
    query = "select "
    nn_columns = []
    for column in not_null_column_list:
        nn_columns.append(f'sum(case when {column} is null then 1 else 0 end) as {column} ')
    query = query + ','.join(nn_columns) + ' from table'
    result = spark.sql(query)
    sum_null_values = sum(result.collect()[0])
    assert sum_null_values == 0, f'{sum_null_values} null values found!!!'


def validate_column_has_supported_values(df, column, supported_values):
    """
    Checks if column only has supported values
    :param df: Spark dataframe
    :param column: column_name
    :param supported_values: supported values list
    :return:
    """
    column_values_list = df.select(column).filter(f'{column} is not Null').distinct().rdd.map(lambda r: r[0]).collect()
    unexpected_values_list = list(set(column_values_list) - set(supported_values))
    assert len(unexpected_values_list) == 0, f'{column} has unexpected values: {unexpected_values_list}!!!'


def validate_max_length_of_column(df, column, max_length):
    """
    Validate the char length of a column's values
    :param df: Dataframe
    :param column: column name
    :param max_length: expected max length
    :return:
    """
    count_outlier = 0
    for search_term in df.filter(f'{column} is not Null').select(column).distinct().rdd.map(lambda r: r[0]).collect():
        if len(search_term) > max_length:
            count_outlier += 1
    assert count_outlier == 0, f'{count_outlier} outliers found!!!'


def validate_column_value_greater_than(df, column, greater_than):
    """
    Counts outliers for column value be greater than a value
    Validates there is no outliers
    The method skip the check for null values as not-null is getting checked in another step
    :param df: dataframe
    :param column: column name of  the dataframe
    :param greater_than: value expected be greater than
    :return:
    """
    count_outlier = df.filter(f'{column} is not Null').where(
        f'{column} = {greater_than} or {column} < {greater_than}').count()
    assert count_outlier == 0, f'{count_outlier} outliers found!!! column: {column}, greater_than: {greater_than}'


def validate_str_date_format(date_text, date_format):
    """
    validate if the date_text matches to the date_format
    :param date_text: date to be validated
    :param format: format to be compared
    :return: True if format matches
    """
    try:
        if date_text != datetime.strptime(date_text, date_format).strftime(date_format):
            raise ValueError
        return True
    except ValueError:
        return False


def validate_date_format_for_column(df, column, date_format):
    """
    Validates date format
    Skip check for null values as not-null is getting checked in another step
    :param df: dataframe
    :param column: column to be checked
    :param date_format: string format of the date
    :return:
    """
    for dt in df.select(column).filter(f'{column} is not Null').distinct().rdd.map(lambda r: r[0]).collect():
        assert validate_str_date_format(str(dt), date_format), f'date format {dt} doesnt match to {date_format}!!!'


def validate_min_value_of_column(df, column, min_value):
    """
    Counts outliers for min value of a column
    Validates there is no outliers
    The method skip the check for null values as not-null is getting checked in another step
    :param df: dataframe
    :param column: column name of  the dataframe
    :param min_value: min value to be expected
    :return:
    """
    count_outlier = df.filter(f'{column} is not Null').where(
        f'{column} < {min_value}').count()
    assert count_outlier == 0, f'{count_outlier} outliers found!!! column: {column}, min_value: {min_value}'


def validate_max_value_of_column(df, column, max_value):
    """
    Counts outliers for max value of a column
    Validates there is no outliers
    The method skip the check for null values as not-null is getting checked in another step
    :param df: dataframe
    :param column: column name of  the dataframe
    :param max_value: max value to be expected
    :return:
    """
    count_outlier = df.filter(f'{column} is not Null').where(
        f'{column} > {max_value}').count()
    assert count_outlier == 0, f'{count_outlier} outliers found!!! column: {column}, max_value: {max_value}'
