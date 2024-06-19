from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def get_coalesce(df, col_val):
    """
    Returns a coalesced column from a DataFrame, converting null values to a specified placeholder.
    Params:
    - df (DataFrame): The DataFrame containing the column to be coalesced.
    - col_val (str): The name of the column to be coalesced.
    Returns:
    - Column: A coalesced column where null values are replaced with a specified placeholder.
    """
    NULL_MAPPER = "_NULL"
    return coalesce(df[col_val].cast("string"), lit(NULL_MAPPER))


def get_query(table, filter):
    """
    Generate SQL queries based on provided table names and optional filter condition.
    Params:
    - table (str): The name of the table.
    - filter (str): Optional filter condition to apply to both tables. If filter is empty, 'null', or None, no filtering is applied.
    
    Returns:
    - query1 (str): The generated SQL query for table.
    """
    if filter == '' or filter == 'null' or filter is None:
        query = f"SELECT * FROM {table}"
    elif filter and isinstance(filter, str):
        query = f"SELECT * FROM {table} WHERE {filter}"
    return query


def get_partition_details(spark, table):
    """
    This fuctions collects partition columns and partition status information for the specified tables.
    Params:
    - spark (SparkSession): The SparkSession object.
    - table (str): The name of the first table.
    
    Returns:
    - partition_status (str): The partition status of the table ('Partitioned' or 'Non-partitioned').
    - table_part_cols (List[str]): The list of partition columns for the table.
    """
    query = f"DESCRIBE {table}"
    
    try: 
        table_df = spark.sql(query)
    except Exception as e:
        return f"Error was encountered while executing the extended DESCRIBE query which is used to collect parition info for the SQL. Error: {str(e)}"

    partition_info = False
    partition_cols = []
    partition_status = 'Non-partitioned'

    for row in table_df.collect():
        if row.col_name and partition_info:
            if row.col_name != '# col_name':
                partition_cols.append(row.col_name)
        elif row.col_name == '# Partition Information':
            partition_info = True
            partition_status = 'Partitioned'
        elif partition_info:
            partition_info = False
            break

    if len(partition_cols) != 0 :
        partition_cols = sorted(partition_cols)

    return partition_status, partition_cols
