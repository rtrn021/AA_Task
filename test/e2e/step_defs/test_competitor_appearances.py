import AA_Task.test.e2e.utils.path_utils as path_utils
import AA_Task.test.e2e.utils.utils as utils
from pytest_bdd import scenarios, given, when, then, parsers
from pyspark.sql.functions import concat
import pandas as pd
import sys
from pathlib import Path
import warnings

sys.path.append(str(Path(__file__).resolve().parent))
warnings.filterwarnings("ignore")

scenarios('../features/competitor_appearances.feature')

source_bucket = 'adthena.data.qa.test'
mapping_spec_path = 'e2e/data/competitor_appearances_ms.xlsx'
mapping_spec = str(path_utils.add_path_to_base_path(mapping_spec_path))

spark = utils.get_spark()

date_format = '%Y-%m-%d'

supported_values_for_columns = {
    'device': ['desktop', 'mobile']
}


@given(parsers.parse('Initiliase the details for table'))
def initialise_details():
    global df_ms
    global pk_column_list
    global not_null_column_list

    df_ms = pd.read_excel(mapping_spec)
    pk_column_list = df_ms[df_ms['pk'] == 'Y']['column_name'].values.tolist()
    not_null_column_list = df_ms[df_ms['not_null'] == 'Y']['column_name'].values.tolist()


@when(parsers.parse('Read source data for {table}'))
def read_data(table):
    global df
    df = spark.read.parquet(f's3a://{source_bucket}/{table}/*.parquet')
    assert df.count()


@then('Validate the schema')
def schema_check():
    utils.compare_validate_schema(df_ms, df)


@then('Validate pk check')
def pk_check():
    utils.validate_pk(spark, df, pk_column_list)


@then('Validate null check')
def null_check():
    utils.validate_not_null_columns(spark, df, not_null_column_list)


@then(parsers.parse('{column} column has only supported values'))
def column_has_only_values(column):
    supported_values = supported_values_for_columns[column]
    utils.validate_column_has_supported_values(df, column, supported_values)


@then(parsers.parse('{column} column max char long <= {max_value}'), converters={"max_value": int})
def max_char_long_of_column(column, max_value):
    utils.validate_max_length_of_column(df, column, max_value)


@then(parsers.parse('{column} column min value >= {min_value}'), converters={"min_value": int})
def column_values_greater_than(column, min_value):
    utils.validate_min_value_of_column(df, column, min_value)


@then(parsers.parse('{column} column max value <= {max_value}'), converters={"max_value": float})
def column_values_greater_than(column, max_value):
    utils.validate_max_value_of_column(df, column, max_value)


@then(parsers.parse('{column} column date format is valid'))
def validate_date_format(column):
    utils.validate_date_format_for_column(df, column, date_format)


@then(parsers.parse('Each row has a corresponding row in scrape_appearances dataset'))
def each_row_has_corresponding():
    global df
    global sa_df
    sa_df = spark.read.parquet(f's3a://{source_bucket}/scrape_appearances/*.parquet')

    # dropping null values from PK (which is not expected to be in dataset)
    sa_df = df.na.drop(subset=['search_term', 'device', 'date'])



    # creating id as a unique key for scrape_appearances
    sa_df = sa_df.select("*", concat(sa_df.search_term, sa_df.device, sa_df.date)
                         .alias("id"))
    sa_df = sa_df.dropDuplicates()

    df = df.select("*", concat(df.search_term, df.device, df.date)
                   .alias("id"))

    sa_df.createOrReplaceTempView('scrape_appearances')
    df.createOrReplaceTempView('competitor_appearances')

    query = "select count(*) from competitor_appearances where id not in (select id from scrape_appearances)"
    result = spark.sql(query)
    assert result.rdd.collect()[0][0] == 0


@then(parsers.parse('sponsored_appearances column max value <= scrape_count of scrape_appearances'))
def validate_sponsored_appearances_max_value():
    global df
    global sa_df

    sa_df.createOrReplaceTempView('scrape_appearances')
    df.createOrReplaceTempView('competitor_appearances')

    query = "select count(*) from (select ca.id, sponsored_appearances, scrape_count from competitor_appearances ca inner join scrape_appearances sa on ca.id = sa.id) as jn where jn.sponsored_appearances > jn.scrape_count"
    result = spark.sql(query)

    assert result.rdd.collect()[0][0] == 0
