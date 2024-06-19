import unittest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from data_validation_framework.process_validations_task import get_data_validation_details


class TestDataValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession for testing
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("unit-testing") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
        
    @patch('builtins.open', new_callable=Mock)
    @patch('your_module.get_query', side_effect=lambda table, filter: f"SELECT * FROM {table} WHERE {filter}" if filter else f"SELECT * FROM {table}")
    @patch('your_module.get_partition_details', side_effect=lambda spark, table: (True, ['col1', 'col2']))
    @patch('your_module.do_count_comparision', return_value=(100, 100, 0, 0.0, True))
    @patch('your_module.do_column_comparison', return_value=(True, ['col1', 'col2'], ['col1', 'col2']))
    @patch('your_module.do_rowdata_comparison', return_value=(0, 0, True, True))
    @patch('your_module.do_metric_sum_comparision', return_value="Successfully stored the metric validation data")
    def test_get_data_validation_details(self, mock_open, mock_get_query, mock_get_partition_details,
                                         mock_do_count_comparision, mock_do_column_comparison,
                                         mock_do_rowdata_comparison, mock_do_metric_sum_comparision):

        # Mock YAML configuration
        yaml_config = """
        rows:
          - table1: "table1"
            table2: "table2"
            filter_condition: "col1 = 'value'"
            is_validation_active: true
            metric_validation_active: true
            dimenssion_columns: "dim_col1, dim_col2"
            dim_metrics_columns: "metric_col1, metric_col2"
            ignored_columns: "ignore_col1, ignore_col2"
            materialization: "materialization_type"
          - table1: "table3"
            table2: "table4"
            filter_condition: "col2 = 'value'"
            is_validation_active: true
            metric_validation_active: false
            materialization: "materialization_type"
        """

        mock_open.return_value.__enter__.return_value.read.return_value = yaml_config

        # Test get_data_validation_details function
        result_df = get_data_validation_details("mock_path")

        # Verify DataFrame schema
        expected_schema = StructType([
            StructField("table1", StringType(), True),
            StructField("table2", StringType(), True),
            StructField("filters", StringType(), True),
            StructField("table1_count", IntegerType(), True),
            StructField("table2_count", IntegerType(), True),
            StructField("count_difference", IntegerType(), True),
            StructField("count_difference_percentage", IntegerType(), True),
            StructField("count_match_status", IntegerType(), True),
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

        self.assertEqual(result_df.schema, expected_schema)

        # Verify mocked function calls
        mock_open.assert_called_once_with("mock_path", 'r')
        mock_get_query.assert_any_call("table1", "col1 = 'value'")
        mock_get_query.assert_any_call("table2", "col1 = 'value'")
        mock_get_query.assert_any_call("table3", "col2 = 'value'")
        mock_get_query.assert_any_call("table4", "col2 = 'value'")
        mock_get_partition_details.assert_any_call(self.spark, "table1")
        mock_get_partition_details.assert_any_call(self.spark, "table2")
        mock_get_partition_details.assert_any_call(self.spark, "table3")
        mock_get_partition_details.assert_any_call(self.spark, "table4")
        mock_do_count_comparision.assert_called()
        mock_do_column_comparison.assert_called()
        mock_do_rowdata_comparison.assert_called()
        mock_do_metric_sum_comparision.assert_called_once_with("table1", "table2", "col1 = 'value'", ["dim_col1", "dim_col2"], ["metric_col1", "metric_col2"], ["ignore_col1", "ignore_col2"])

if __name__ == "__main__":
    unittest.main()