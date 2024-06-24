from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.operations import *
from data_validation_framework.common import get_dataframe
import yaml


spark = SparkSession.builder.appName("data validation framework") \
                            .config("spark.dynamicAllocation.enabled", "true") \
                            .config("spark.sql.shuffle.partitions", "800") \
                            .config("spark.default.parallelism", "600") \
                            .config("spark.executor.memory", "20g") \
                            .config("spark.driver.memory", "9g") \
                            .enableHiveSupport().getOrCreate()


def get_data_validation_details(table1, table2, filter_condition='', metric_validation_active=False, dimenssion_columns='', dim_metrics_columns='', ignored_columns='', is_validation_active=True, materialization=None):
    """
    Process the count, row and metric(optional) data validation for the single set of table taking the below inputs from user function.
    Params:
    - result_rows (list): Empty list collecting the validation data.
    - table1 (str): first table name.
    - table2 (str): second table name.
    - filter (str): filter condition to b applied.
    - metric_validation_active (bool): metric_validation_active flag.
    - dimenssion_cols (str): Specify the dimension or slice columns(common in both tables) you wish to include in the validation query using this parameter.
    - dim_metrics_cols (str): columns (common in both tables) to be converted as metric columns (perform count agg) in the validation query. 
    - ignored_cols (ste): columns that you wish to ignore/exclude from the metrics of integral type.
    - is_validation_active (bool): Flag to process the validation. True/False.
    - materialization (str): Type of table string.
    Returns:
    - df (dataframe): The output dataframe containg the validation details for each input table provided in yaml.
    """
    result_rows = []
    result_rows = validation_process(spark, result_rows, table1, table2, filter_condition, metric_validation_active, dimenssion_columns, dim_metrics_columns, ignored_columns, is_validation_active, materialization)
    df = get_dataframe(spark, result_rows)
    return df


def get_multiple_table_data_validation_details(path):
    """
    Process the count, row and metric(optional) data validation taking the input yaml from user.
    Params:
    - path (dict): Dictionary containing the configuration for tables to be comapare including table names, filter conditions, etc.
    Returns:
    - df (dataframe): The output dataframe containg the validation details for each input table provided in yaml.
    """

    config = yaml.safe_load(open(path, 'r'))
    input_rows = config['rows']
    result_rows = []
    for row in input_rows:
        table1 = row['table1']
        table2 = row['table2']
        filter = row.get('filter_condition', '')
        is_validation_active = row.get('is_validation_active', True)
        metric_validation_active = row.get('metric_validation_active', False)
        dimenssion_columns =  row.get('dimenssion_columns', '')
        dim_metrics_columns =  row.get('dim_metrics_columns', '')
        ignored_columns =  row.get('ignored_columns', '')
        materialization = row.get('materialization', '')
        
        result_rows = validation_process(spark, result_rows, table1, table2, filter, metric_validation_active, dimenssion_columns, dim_metrics_columns, ignored_columns, is_validation_active, materialization)
 
    df = get_dataframe(spark, result_rows)
    return df
