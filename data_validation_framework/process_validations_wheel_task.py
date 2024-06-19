import argparse
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.process_validations_task import get_data_validation_details


def get_data_validation_details_wheel_task():
    """
    This is python wheel task entry point using the logic from notebook task function get_data_validation_details().
    """
    parser = argparse.ArgumentParser(description='process validation')
    parser.add_argument('--path', type=str, help='file path of yaml containing validation input parameters ', required=True)
    args = parser.parse_args()
    
    path = str(args.path)
    validation_df = get_data_validation_details(path)
    validation_df.show()
    return validation_df
