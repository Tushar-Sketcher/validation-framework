import argparse
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.process_validations_task import get_data_validation_details, get_multiple_table_data_validation_details


def get_multiple_table_data_validation_details_wheel_task():
    """
    This is python wheel task entry point using the logic from notebook task function get_multiple_table_data_validation_details().
    """
    parser = argparse.ArgumentParser(description='process validation')
    parser.add_argument('--path', type=str, help='file path of yaml containing validation input parameters ', required=True)
    args = parser.parse_args()
    
    path = str(args.path)
    validation_df = get_multiple_table_data_validation_details(path)
    validation_df.show(truncate=0)
    return validation_df


def get_data_validation_details_wheel_task():
    """
        This is python wheel task entry point using the logic from notebook task function get_data_validation_details() 
        to fetch validation data for the single set of tables. 
    """
    parser = argparse.ArgumentParser(description='process validation')
    parser.add_argument('--table1', type=str, help='file path of yaml containing validation input parameters ', required=True)
    parser.add_argument('--table2', type=str, help='file path of yaml containing validation input parameters ', required=True)
    parser.add_argument('--filter_condition', type=str, help='file path of yaml containing validation input parameters ', required=True)
    parser.add_argument('--metric_validation_active', type=str, help='file path of yaml containing validation input parameters ', required=True)
    parser.add_argument('--dimenssion_columns', type=str, help='file path of yaml containing validation input parameters ', required=True)
    parser.add_argument('--dim_metrics_columns', type=str, help='file path of yaml containing validation input parameters ', required=True)
    parser.add_argument('--ignored_columns', type=str, help='file path of yaml containing validation input parameters ', required=True)
    parser.add_argument('--is_validation_active', type=str, help='file path of yaml containing validation input parameters ', required=True)
    parser.add_argument('--materialization', type=str, help='file path of yaml containing validation input parameters ', required=True)
    args = parser.parse_args()

    
    table1 = args.table1
    table2 = args.table2
    filter_condition = args.filter_condtition
    metric_validation_active = args.metric_validation_active
    dimenssion_columns = args.dimenssion_columns
    dim_metrics_columns = args.dim_metrics_columns
    ignored_columns = args.ignored_columns
    is_validation_active = args.is_validation_active
    materialization = args.materialization

    validation_df = get_data_validation_details(table1, table2, filter_condition, metric_validation_active, dimenssion_columns, dim_metrics_columns, ignored_columns, is_validation_active, materialization)
    validation_df.show(truncate=0)
    return validation_df
