from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.common import get_coalesce


class ValidationError(Exception):
    def __init__(self, message):
        """
        Initialize a new ValidationError.

        Params:
        - message (str): The error message.
        """
        self.message = message
        super().__init__(self.message)


def do_count_comparision(table1_df, table2_df):
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

    diff_per = float('inf') if table1_count == 0 else (diff/table1_count) * 100
    
    cnt_match_st = True if table1_count == table2_count else False

    return table1_count, table2_count, diff, diff_per, cnt_match_st


def do_column_comparison(table1_df, table2_df):
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


def do_rowdata_comparison(table1_df, table2_df):
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
    table1_md5 = table1_df.select("checksum").distinct()
    table2_md5 = table2_df.select("checksum").distinct()

    table1_md5 = table1_md5.sort("checksum", ascending=True)
    table2_md5 = table2_md5.sort("checksum", ascending=True)

    table1_subtract_count = table1_md5.subtract(table2_md5).count()
    table2_subtract_count = table2_md5.subtract(table1_md5).count()

    table1_datamatch_status = table1_md5.subtract(table2_md5).isEmpty()
    table2_datamatch_status = table2_md5.subtract(table1_md5).isEmpty()

    return table1_subtract_count, table2_subtract_count, table1_datamatch_status, table2_datamatch_status


def do_metric_sum_comparision(spark, table1, table2, filter, dimenssion_cols="", dim_metrics_cols="", ignored_cols=""):
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
    tb1_metric_cols = sorted([col_name.lower() for col_name, data_type in tb1.dtypes if data_type in ['bigint','int','double','float']])
    tb2_metric_cols = sorted([col_name.lower() for col_name, data_type in tb2.dtypes if data_type in ['bigint','int','double','float']])

    for value in ignored_cols:
        tb1_metric_cols = [col for col in tb1_metric_cols if col != value]
        tb2_metric_cols = [col for col in tb2_metric_cols if col != value]

    try:
        if not tb1_metric_cols and not tb2_metric_cols:
            raise ValidationError(f"ERROR ::: tables {table1} or {table2} does not contain any metric columns of type 'bigint','int','double'. Can not proceed for the validation sql query generation for Table1 and Table2")
        elif tb1_metric_cols != tb2_metric_cols:
            raise ValidationError(f"ERROR ::: Retrived metric columns for {table1} and {table2} are not same. Please verrify the table columns. Can not proceed for the validation sql query generation for Table1 and Table2 tables.\n. {table1} metric columns count: {len(tb1_metric_cols)} Column List: {tb1_metric_cols} \n {table2} metric columns count: {len(tb2_metric_cols)} Columns List: {tb2_metric_cols}")
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
            metric_table = f'sandbox_marketing.mart_mops_test.' + table2.split('.')[2] + '_metric_validation'
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