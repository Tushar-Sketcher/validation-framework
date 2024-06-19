import unittest
from unittest.mock import Mock
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from data_validation_framework.common import get_coalesce, get_query, get_partition_details

class TestFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession for testing
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("unit-testing") \
            .getOrCreate()


    def test_get_coalesce(self):
        # Mock DataFrame
        df_mock = Mock()
        df_mock.col_val = "value"
        df_mock.cast.return_value = df_mock

        # Test get_coalesce function
        result = get_coalesce(df_mock, "col_val")
        self.assertEqual(result, lit("_NULL"))

    
    def test_get_query(self):
        # Test get_query function with different scenarios
        self.assertEqual(get_query("table1", ""), "SELECT * FROM table1")
        self.assertEqual(get_query("table2", "col1 = 'value'"), "SELECT * FROM table2 WHERE col1 = 'value'")
        self.assertEqual(get_query("table3", None), "SELECT * FROM table3")


    def test_get_partition_details(self):
        # Mock SparkSession and DataFrame
        spark_mock = Mock(spec=self.spark)
        df_mock = spark_mock.sql.return_value
        df_mock.collect.return_value = [
            Mock(col_name="# Partition Information"),
            Mock(col_name="partition_col1"),
            Mock(col_name="partition_col2"),
            Mock(col_name="other_col")
        ]

        # Test get_partition_details function
        result_status, result_cols = get_partition_details(spark_mock, "mock_table")
        self.assertEqual(result_status, "Partitioned")
        self.assertEqual(result_cols, ["partition_col1", "partition_col2"])

if __name__ == '__main__':
    unittest.main()
