from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.common import *
from pyspark.storagelevel import StorageLevel


class ValidationError(Exception):
    def __init__(self, message):
        """
        Initialize a new ValidationError.

        Params:
        - message (str): The error message.
        """
        self.message = message
        super().__init__(self.message)


def compare_counts(table1_df, table2_df):
    """
    Performs count validations on dataframes based on configuration parameters provided in yaml file as input and returns a DataFrame.
    The function iterates through the rows in the provided configuration and performs count validations on the specified tables.

    Params:
    - table1_df (dataframe): First table dataframe.
    - table2_df (dataframe): Second table dataframe.
    Returns:
    - table1_count, table2_count, diff, diff_per, cnt_match_st: variables containing count validation details.
    """
    table1_count = table1_df.count()
    table2_count = table2_df.count()

    diff = table1_count - table2_count
    diff_per = None if table1_count == 0 else (diff/table1_count) * 100
    cnt_match_st = True if table1_count == table2_count else False

    return table1_count, table2_count, diff, diff_per, cnt_match_st


def compare_columns(table1_df, table2_df):
    """
    Performs ddl check fetching the dataframe columns and verifying if those columns are same.
    Params:
    - table1_df (dataframe): First table dataframe.
    - table2_df (dataframe): Second table dataframe.
    Returns:
    - col_match_st, table1_sort_cols, table2_sort_cols: variables containing count validation details.
    """
    table1_sort_cols = sorted(table1_df.columns)
    table2_sort_cols = sorted(table2_df.columns)

    col_match_status = True if table1_sort_cols == table2_sort_cols else False

    return col_match_status, table1_sort_cols, table2_sort_cols


def compare_rows(table1_df, table2_df):
    """
    Perform row-level validation between two tables using our MD5 hashing approach.
    Params:
    - table1_df (dataframe): First table dataframe.
    - table2_df (dataframe): Second table dataframe.
    Returns:
    - table1_subtract_cnt, table2_subtract_cnt, table1_datamatch_st, table2_datamatch_st: variables containg the row validation details.
    """
    table1_df =  table1_df.withColumn("checksum", md5(concat_ws("", *[get_coalesce(table1_df, col_val) 
                                                                            for col_val in table1_df.columns])))
    table2_df =  table2_df.withColumn("checksum", md5(concat_ws("", *[get_coalesce(table2_df, col_val) 
                                                                            for col_val in table2_df.columns])))
    table1_md5 = table1_df.select("checksum").distinct().orderBy("checksum")
    table2_md5 = table2_df.select("checksum").distinct().orderBy("checksum")

    table1_md5.persist(StorageLevel.MEMORY_AND_DISK)
    table2_md5.persist(StorageLevel.MEMORY_AND_DISK)

    table1_subtract_count = table1_md5.subtract(table2_md5).count()
    table2_subtract_count = table2_md5.subtract(table1_md5).count()

    table1_datamatch_status = table1_md5.subtract(table2_md5).isEmpty()
    table2_datamatch_status = table2_md5.subtract(table1_md5).isEmpty()

    table1_md5.unpersist()
    table2_md5.unpersist()

    return table1_subtract_count, table2_subtract_count, table1_datamatch_status, table2_datamatch_status


def compare_metrics(table1, table2, filter="", dimenssion_cols="", dim_metrics_cols="", ignored_cols="", spark=""):
    """
    Perform metric data validations between two tables.
    Params: 
    - spark (sparksession): Sparksession.
    - table1 (str): Name of the first table.
    - table2 (str): Name of the second table.
    - filter (str): Filter condition to be applied for the tables.
    - dimenssion_cols (str): List of dimension columns with comma seperated values to include in the validation.
    - dim_metrics_cols (str): List of dimension metrics columns with comma seperated values for comparison.
    - ignored_cols (str): List of columns with comma seperated values to be ignored in the comparison.
    Returns:
    - DataFrame: Resulting DataFrame after performing validations.
    """
    dimenssion_cols = [] if dimenssion_cols == "" else list(dimenssion_cols.split(', '))
    dim_metrics_cols = [] if dim_metrics_cols == "" else list(dim_metrics_cols.split(', '))
    ignored_cols = [] if ignored_cols == "" else list(ignored_cols.split(', '))

    tb1_sql = f"SELECT * FROM {table1} LIMIT 1"
    tb2_sql = f"SELECT * FROM {table2} LIMIT 1"

    tb1 = spark.sql(tb1_sql)
    tb2 = spark.sql(tb2_sql)
    tb1_metric_cols = sorted([col_name.lower() for col_name, data_type in tb1.dtypes if data_type.startswith(('bigint', 'int', 'double', 'float', 'decimal'))])
    tb2_metric_cols = sorted([col_name.lower() for col_name, data_type in tb2.dtypes if data_type.startswith(('bigint', 'int', 'double', 'float', 'decimal'))])

    for value in ignored_cols:
        tb1_metric_cols = [col for col in tb1_metric_cols if col != value]
        tb2_metric_cols = [col for col in tb2_metric_cols if col != value]

    try:
        if not tb1_metric_cols and not tb2_metric_cols:
            raise ValidationError(f"ERROR ::: Neither {table1} nor {table2} contains any metric columns of type 'bigint','int','double' or 'float'. Validation SQL query generation cannot proceed for these tables.")
        elif tb1_metric_cols != tb2_metric_cols:
            raise ValidationError(f"ERROR ::: The metric columns retrieved for {table1} and {table2} are not identical. Please verify both table's columns before proceeding.\n{table1} No of metrics: {len(tb1_metric_cols)} columns: {tb1_metric_cols}\n{table2} No of metrics: {len(tb2_metric_cols)} columns: {tb2_metric_cols}")
        else:
            sum_agg_tb1 = [f"COALESCE(SUM({col}), 0) AS {col}_tb1_sum" for col in tb1_metric_cols]
            sum_agg_tb2 = [f"COALESCE(SUM({col}), 0) AS {col}_tb2_sum" for col in tb2_metric_cols]

            metric_dim_agg_tb1 = [f"COALESCE(COUNT(DISTINCT {col}), 0) AS {col}_tb1_cnt" for col in dim_metrics_cols]
            metric_dim_agg_tb2 = [f"COALESCE(COUNT(DISTINCT {col}), 0) AS {col}_tb2_cnt" for col in dim_metrics_cols]

            dim_metrics_diff_final = [(
                            f"{col}_tb1_cnt",
                            f"{col}_tb2_cnt",
                            f"ROUND(COALESCE({col}_tb1_cnt, 0) - COALESCE({col}_tb2_cnt, 0), 4) AS {col}_diff",
                            f"ROUND(((COALESCE({col}_tb1_cnt, 0) - COALESCE({col}_tb2_cnt, 0))/COALESCE({col}_tb1_cnt, 0))*100, 4) AS per_diff_{col}"
                            ) for col in dim_metrics_cols ]

            metric_diff_final = [(
                    f"ROUND({col}_tb1_sum,4) AS {col}_tb1_sum",
                    f"ROUND({col}_tb2_sum,4) AS {col}_tb2_sum",
                    f"ROUND(COALESCE({col}_tb1_sum, 0) - COALESCE({col}_tb2_sum, 0), 4) AS {col}_diff",
                    f"ROUND(((COALESCE({col}_tb1_sum, 0) - COALESCE({col}_tb2_sum, 0))/COALESCE({col}_tb1_sum, 0))*100, 4) AS per_diff_{col}"
                    ) for col in tb1_metric_cols]

            join_cols = [f"table1.{col} = table2.{col}" for col in dimenssion_cols]

            sql = """
                    WITH table1 AS (
                        SELECT 
                            {cols1} 
                        FROM {table1}
                        {where}
                        {groupby}
                    ),
                    table2 AS (
                        SELECT 
                            {cols2} 
                        FROM {table2}
                        {where}
                        {groupby}
                    ),
                    final AS (
                        SELECT 
                            {dim_cols} 
                            {dim_metrics_diff_final}
                            {metric_diff_final}
                        FROM table1
                        FULL JOIN table2
                        {join_conditions}
                    )
                    SELECT * FROM final""".format(table1=table1,
                                                  table2=table2,
                                                  cols1 = ", ".join(dimenssion_cols + metric_dim_agg_tb1 + sum_agg_tb1),
                                                  cols2=", ".join(dimenssion_cols + metric_dim_agg_tb2 + sum_agg_tb2),
                                                  where=' WHERE ' + filter if filter else '',
                                                  groupby=' GROUP BY ' + ", ".join(dimenssion_cols) + ' ORDER BY ' + ", ".join(dimenssion_cols) + ' DESC' if dimenssion_cols else '',
                                                  dim_cols = ", ".join([f"COALESCE(table1.{col}, table2.{col}) AS {col}," for col in dimenssion_cols]),
                                                  dim_metrics_diff_final=", ".join([", ".join(diff_cols) for diff_cols in dim_metrics_diff_final]) + ',' if dim_metrics_diff_final else '',
                                                  metric_diff_final=", ".join([", ".join(diff_col) for diff_col in metric_diff_final]),
                                                  join_conditions=' ON ' + ' AND '.join(join_cols) if join_cols else '')

            print("Please Find The Validation Query Below ::::\n")
            print(sql)
            print('\n**************************** Column Comparison Summary ****************************')
            print(f'List Of Metric Columns Compared From {table1} and {table2} Table: {dim_metrics_cols + tb1_metric_cols}\n')
            print(f'List Of Dimenssion Columns Added From the tables: {dimenssion_cols}\n')
            print(f'Check dataframe for the query results:::')
            metric_df = spark.sql(sql)
            metric_table = "{catalog}.{schema}.{table_name}_metric_validation".format(catalog='sandbox_marketing',
                                                                                      schema='mart_mops_test',
                                                                                      table_name=table2.split('.')[2])
            spark.sql(f"DROP TABLE IF EXISTS {metric_table}")
            try:
                metric_df.write.format("delta").mode("overwrite").saveAsTable(metric_table)
                print(f"Successfully stored the metric validation data in table: {metric_table}. Please query this table to check validation details.")
                return metric_df
            except Exception as e:
                return f"Error encountered while writing metric validation data. Error Details: {str(e)}"

    except ValidationError as ve:
            print("ValidationError:", ve.message)
            return


def validation_process(spark, result_rows, table1, table2, filter, metric_validation_active, dimenssion_cols, dim_metrics_cols, ignored_cols, is_validation_active, materialization):
    """
    Process the count, row and metric(optional) data validation based on below parameters input.
    Params:
    - spark (sparksession): Sparksession.
    - result_rows (list): Empty list collecting the validation data.
    - table1 (str): first table name.
    - table2 (str): second table name.
    - filter (str): filter condition to b applied.
    - metric_validation_active (bool): metric_validation_active flag.
    - dimenssion_cols (str): Specify the dimension or slice columns(common in both tables) you wish to include in the validation query using this parameter.
    - dim_metrics_cols (str): columns (common in both tables) to be converted as metric columns (perform count agg) in the validation query. 
    - ignored_cols (ste): columns that you wish to ignore/exclude from the metrics of integral type.
    - is_validation_active (bool): Flag to process the validation. True/False.
    - materialization (str): Type of table string
     Returns:
    - result_rows (list): The list of rows containing validation details.
    """
    try:
        if is_validation_active:
            table1_query = get_query(table1, filter)
            table2_query = get_query(table2, filter)

            partition_status_table1, partition_columns_table1 = get_partition_details(spark, table1)
            partition_status_table2, partition_columns_table2 = get_partition_details(spark, table2) 

            table_df1 = spark.sql(table1_query)
            table_df2 = spark.sql(table2_query)

            table_df1.persist(StorageLevel.MEMORY_AND_DISK)
            table_df2.persist(StorageLevel.MEMORY_AND_DISK)

            table1_count, table2_count, diff, diff_per, count_match_status = compare_counts(table_df1, table_df2)
            column_match_status, table1_sort_columns, table2_sort_columns = compare_columns(table_df1, table_df2)

            if column_match_status:
                table_df1 = table_df1.select(*table1_sort_columns)
                table_df2 = table_df2.select(*table2_sort_columns)

                table1_subtract_count, table2_subtract_count, table1_datamatch_status, table2_datamatch_status = compare_rows(table_df1, table_df2)
                result_rows.append((table1, table2, filter, table1_count, table2_count, diff, diff_per, count_match_status, table1_subtract_count, 
                            table2_subtract_count, table1_datamatch_status, table2_datamatch_status, partition_status_table1, 
                            partition_status_table2, partition_columns_table1, partition_columns_table2, materialization))

                if metric_validation_active:
                    compare_metrics(table1, table2, filter, dimenssion_cols, dim_metrics_cols, ignored_cols, spark)

            else:
                print(f"- The Row Data Validation process skipped, as the Columns of the tables {table1} and {table2} are not identical. Please verify the columns of the tables.")
                print(f"{table1} no of column {len(table1_sort_columns)} and table columns list:: {table1_sort_columns}")
                print(f"{table2} no of column {len(table2_sort_columns)} and table columns list:: {table2_sort_columns}")

                print(f"- Row Data validation related columns will be set to NULL in the final result set for these tables.\n")
                table1_subtract_count = None 
                table2_subtract_count = None
                table1_datamatch_status = None 
                table2_datamatch_status = None
                result_rows.append((table1, table2, filter, table1_count, table2_count, diff, diff_per, count_match_status, table1_subtract_count, 
                                    table2_subtract_count, table1_datamatch_status, table2_datamatch_status, partition_status_table1, 
                                    partition_status_table2, partition_columns_table1, partition_columns_table2, materialization))
            table_df1.unpersist()
            table_df2.unpersist()
        else:
            print(f"- Validation process skipped for the {table1} and {table2} due to 'False' or invalid value provided for 'is_validation_active' parameter.\n")

    except Exception as e:
        print(f"Error encountered for the {table1} and {table2} When executing validation process:\n{str(e)}")
        print(f"Omitting this row from being included in the final DataFrame\n")

    return result_rows
