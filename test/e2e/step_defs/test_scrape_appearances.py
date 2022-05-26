from pytest_bdd import scenarios, given, when, then, parsers
import AA_Task.test.e2e.utils.path_utils as path_utils
import AA_Task.test.e2e.utils.utils as utils
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

scenarios('../features/scrape_appearances.feature')


source_bucket = 'adthena.data.qa.test'
mapping_spec_path = 'e2e/data/scrape_appearances_ms.xlsx'
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
    utils.validate_column_has_supported_values(df,column,supported_values)


@then(parsers.parse('{column} column max char long <= {max_value}'), converters={"max_value": int})
def max_char_long_of_column(column, max_value):
    utils.validate_max_length_of_column(df, column, max_value)


@then(parsers.parse('{column} column values > {greater_than}'), converters={"greater_than": int})
def column_values_greater_than(column, greater_than):
    utils.validate_column_value_greater_than(df, column, greater_than)

@then(parsers.parse('{column} column date format is valid'))
def validate_date_format(column):
    utils.validate_date_format_for_column(df, column, date_format)