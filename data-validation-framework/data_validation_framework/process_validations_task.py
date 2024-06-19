from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.operations import *
from data_validation_framework.common import get_query, get_partition_details
import yaml


spark = SparkSession.builder.appName("data validation framework").enableHiveSupport().getOrCreate()


def get_data_validation_details(path):
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
        try:
            if row.get('is_validation_active', True):
                table1, table2, filter = row['table1'], row['table2'], row.get('filter_condition', '')
                table1_query = get_query(table1, filter)
                table2_query = get_query(table2, filter)

                partition_status_table1, partition_columns_table1  = get_partition_details(spark, table1)
                partition_status_table2, partition_columns_table2  = get_partition_details(spark, table2) 
                
                table_df1 = spark.sql(table1_query)
                table_df2 = spark.sql(table2_query)

                table1_count, table2_count, diff, diff_per, count_match_status = do_count_comparision(table_df1, table_df2)
                column_match_status, table1_sort_columns, table2_sort_columns = do_column_comparison(table_df1, table_df2)


                if count_match_status and column_match_status:
                    table_df1 = table_df1.select(*table1_sort_columns)
                    table_df2 = table_df2.select(*table2_sort_columns)

                    table1_subtract_count, table2_subtract_count, table1_datamatch_status, table2_datamatch_status = do_rowdata_comparison(table_df1, table_df2)
                    result_rows.append((table1, table2, filter, table1_count, table2_count, diff, diff_per, count_match_status, table1_subtract_count, 
                                table2_subtract_count, table1_datamatch_status, table2_datamatch_status, partition_status_table1, 
                                partition_status_table2, partition_columns_table1, partition_columns_table2, row.get('materialization', None)))

                    if row.get('metric_validation_active', False):
                        if table1_datamatch_status == False or table2_datamatch_status == False:
                            dimenssion_cols =  row.get('dimenssion_columns', '')
                            dim_metrics_cols =  row.get('dim_metrics_columns', '')
                            ignored_cols =  row.get('ignored_columns', '')
                            do_metric_sum_comparision(spark, table1, table2, filter, dimenssion_cols, dim_metrics_cols, ignored_cols)
                else:
                    raise ValidationError(f"Error occured while processing Row Data Comparison for tables : {table1} and {table2}.")

            else:
                print(f"- Validation process skipped for the row : {row} due to 'False' or invalid value provided for 'is_validation_active'.\n")
        except ValidationError as ve:
            print("ValidationError:", ve.message)
            if count_match_status is False:
                print(f"- The Row Data Validation process skipped, as the Counts of the tables do not match for {table1} and {table2}.")
            elif column_match_status is False:
                print(f"- The Row Data Validation process skipped, as the Columns of the tables do not match for {table1} and {table2}. Please verify the columns are same in both tables.")
                print(f"{table1} no of column {len(table1_sort_columns)} and table columns list:: {table1_sort_columns}")
                print(f"{table2} no of column {len(table2_sort_columns)} and table columns list:: {table2_sort_columns}")

            print(f"- Row Data validation related columns will be set to NULL in the final result set for these tables.\n")
            table1_subtract_count = None 
            table2_subtract_count = None
            table1_datamatch_status = None 
            table2_datamatch_status = None
            result_rows.append((table1, table2, filter, table1_count, table2_count, diff, diff_per, count_match_status, table1_subtract_count, 
                                table2_subtract_count, table1_datamatch_status, table2_datamatch_status, partition_status_table1, 
                                partition_status_table2, partition_columns_table1, partition_columns_table2, row.get('materialization', None)))
            continue
        except Exception as e:
            print(f"Error encountered for the row {row} When executing validation process:\n{str(e)}")
            print(f"Omitting this row from being included in the final DataFrame\n")
            continue

    schema = StructType([
                StructField("table1", StringType(), True),
                StructField("table2", StringType(), True),
                StructField("filters", StringType(), True),
                StructField("table1_count", IntegerType(), True),
                StructField("table2_count", IntegerType(), True),
                StructField("count_difference", IntegerType(), True),
                StructField("count_difference_percentage", FloatType(), True),
                StructField("count_match_status", BooleanType(), True),
                StructField("table1_subtract_count", IntegerType(), True),
                StructField("table2_subtract_count", IntegerType(), True),
                StructField("table1_datamatch_status", BooleanType(), True),
                StructField("table2_datamatch_status", BooleanType(), True),
                StructField("partition_status_table1", StringType(), True),
                StructField("partition_status_table2", StringType(), True),
                StructField("table1_partition_columns", ArrayType(StringType()), True),
                StructField("table2_partition_columns", ArrayType(StringType()), True),
                StructField("materialization", StringType(), True),

            ])

    df = spark.createDataFrame(result_rows, schema)
    return df