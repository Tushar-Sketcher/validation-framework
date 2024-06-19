import unittest
from unittest.mock import Mock
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, md5
from data_validation_framework.operations import ValidationError, do_count_comparision, do_column_comparison, do_rowdata_comparison, do_metric_sum_comparision

class TestFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession for testing
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("unit-testing") \
            .getOrCreate()

    def test_do_count_comparision(self):
        # Mock DataFrames
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.count.return_value = 100
        df2_mock.count.return_value = 95

        # Test do_count_comparision function
        result = do_count_comparision(df1_mock, df2_mock)
        self.assertEqual(result, (100, 95, 5, 5.0, False))

    def test_do_column_comparison(self):
        # Mock DataFrames
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2']
        df2_mock.columns = ['col2', 'col1']

        # Test do_column_comparison function
        result = do_column_comparison(df1_mock, df2_mock)
        self.assertFalse(result[0])
        self.assertEqual(result[1], ['col1', 'col2'])
        self.assertEqual(result[2], ['col1', 'col2'])

    def test_do_rowdata_comparison(self):
        # Mock DataFrames
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2']
        df2_mock.columns = ['col1', 'col2']
        df1_mock.withColumn.return_value = df1_mock
        df2_mock.withColumn.return_value = df2_mock
        df1_mock.select.return_value.distinct.return_value = df1_mock
        df2_mock.select.return_value.distinct.return_value = df2_mock
        df1_mock.sort.return_value = df1_mock
        df2_mock.sort.return_value = df2_mock
        df1_mock.subtract.return_value.count.return_value = 0
        df2_mock.subtract.return_value.count.return_value = 0

        # Test do_rowdata_comparison function
        result = do_rowdata_comparison(df1_mock, df2_mock)
        self.assertEqual(result, (0, 0, True, True))

    def test_do_metric_sum_comparision(self):
        # Mock SparkSession
        spark_mock = Mock(spec=self.spark)
        spark_mock.sql.return_value = Mock()
        spark_mock.sql.return_value.dtypes = [('col1', 'bigint'), ('col2', 'int')]
        spark_mock.sql.return_value.columns = ['col1', 'col2']
        spark_mock.sql.return_value.count.return_value = 1
        spark_mock.sql.return_value.sort.return_value = spark_mock.sql.return_value
        spark_mock.sql.return_value.subtract.return_value = spark_mock.sql.return_value
        spark_mock.sql.return_value.isEmpty.return_value = True
        spark_mock.sql.return_value.write.return_value = spark_mock.sql.return_value

        # Test do_metric_sum_comparision function
        result = do_metric_sum_comparision(spark_mock, "table1", "table2", "filter")
        self.assertIsInstance(result, str)
        self.assertIn("Successfully stored", result)

    def test_do_metric_sum_comparision_with_validation_error(self):
        # Mock SparkSession
        spark_mock = Mock(spec=self.spark)
        spark_mock.sql.return_value = Mock()
        spark_mock.sql.return_value.dtypes = [('col1', 'string')]
        spark_mock.sql.return_value.columns = ['col1']
        spark_mock.sql.return_value.count.return_value = 1

        # Test do_metric_sum_comparision function with ValidationError
        with self.assertRaises(ValidationError):
            do_metric_sum_comparision(spark_mock, "table1", "table2", "filter")

if __name__ == "__main__":
    unittest.main()

