import unittest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, md5
from data_validation_framework.operations import ValidationError, compare_counts, compare_columns, compare_rows, compare_metrics

class TestFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("data validation framework") \
            .getOrCreate()

    ## Test cases for compare_counts()
    def test_compare_counts(self):
        # Mock DataFrames
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.count.return_value = 100
        df2_mock.count.return_value = 95

        # Test compare_counts function
        result = compare_counts(df1_mock, df2_mock)
        self.assertEqual(result, (100, 95, 5, 5.0, False))

        # Mock DataFrames where counts are equal
        df1_mock_equal = Mock()
        df2_mock_equal = Mock()
        df1_mock_equal.count.return_value = 100
        df2_mock_equal.count.return_value = 100

        # Test compare_counts function for equal counts
        result_equal = compare_counts(df1_mock_equal, df2_mock_equal)
        self.assertEqual(result_equal, (100, 100, 0, 0.0, True))

        # Mock DataFrames where one count is zero
        df1_mock_zero = Mock()
        df2_mock_zero = Mock()
        df1_mock_zero.count.return_value = 0
        df2_mock_zero.count.return_value = 100

        # Test compare_counts function for one DataFrame having zero count
        result_zero = compare_counts(df1_mock_zero, df2_mock_zero)
        self.assertEqual(result_zero, (0, 100, 100, float('inf'), False))

    ## Test cases for compare_columns()
    def test_compare_columns_same_order(self):
        # Mock DataFrames with columns in the same order
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2']
        df2_mock.columns = ['col1', 'col2']

        # Test compare_columns function
        result = compare_columns(df1_mock, df2_mock)
        self.assertTrue(result[0])  # Expect columns to match
        self.assertEqual(result[1], ['col1', 'col2'])  # Expect sorted columns from df1
        self.assertEqual(result[2], ['col1', 'col2'])  # Expect sorted columns from df2

    def test_compare_columns_different_order(self):
        # Mock DataFrames with columns in different order
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2']
        df2_mock.columns = ['col2', 'col1']

        # Test compare_columns function
        result = compare_columns(df1_mock, df2_mock)
        self.assertFalse(result[0])  # Expect columns to not match due to order difference
        self.assertEqual(result[1], ['col1', 'col2'])  # Expect sorted columns from df1
        self.assertEqual(result[2], ['col1', 'col2'])  # Expect sorted columns from df2

    def test_compare_columns_extra_columns(self):
        # Mock DataFrames with extra columns in df2
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2']
        df2_mock.columns = ['col1', 'col2', 'col3']

        # Test compare_columns function
        result = compare_columns(df1_mock, df2_mock)
        self.assertFalse(result[0])  # Expect columns to not match due to extra column in df2
        self.assertEqual(result[1], ['col1', 'col2'])  # Expect sorted columns from df1
        self.assertEqual(result[2], ['col1', 'col2', 'col3'])  # Expect sorted columns from df2 including extra column

    def test_compare_columns_missing_columns(self):
        # Mock DataFrames with missing columns in df2
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2', 'col3']
        df2_mock.columns = ['col1']

        # Test compare_columns function
        result = compare_columns(df1_mock, df2_mock)
        self.assertFalse(result[0])  # Expect columns to not match due to missing columns in df2
        self.assertEqual(result[1], ['col1', 'col2', 'col3'])  # Expect sorted columns from df1
        self.assertEqual(result[2], ['col1'])  # Expect sorted columns from df2

    ## Test cases for compare_rows()
    def test_compare_rows(self):
        # Mock DataFrames for the case where rows match
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

        # Test compare_rows function
        result = compare_rows(df1_mock, df2_mock)
        self.assertEqual(result, (0, 0, True, True))

        # Mock DataFrames for the case where rows do not match
        df1_mock_no_match = Mock()
        df2_mock_no_match = Mock()
        df1_mock_no_match.columns = ['col1', 'col2']
        df2_mock_no_match.columns = ['col1', 'col2']
        df1_mock_no_match.withColumn.return_value = df1_mock_no_match
        df2_mock_no_match.withColumn.return_value = df2_mock_no_match
        df1_mock_no_match.select.return_value.distinct.return_value = df1_mock_no_match
        df2_mock_no_match.select.return_value.distinct.return_value = df2_mock_no_match
        df1_mock_no_match.sort.return_value = df1_mock_no_match
        df2_mock_no_match.sort.return_value = df2_mock_no_match
        df1_mock_no_match.subtract.return_value.count.return_value = 1
        df2_mock_no_match.subtract.return_value.count.return_value = 2

        # Test compare_rows function for non-matching rows
        result_no_match = compare_rows(df1_mock_no_match, df2_mock_no_match)
        self.assertEqual(result_no_match, (1, 2, False, False))

    ## Test cases for compare_metrics()
    def setUp(self):
        # Mock SparkSession setup
        self.spark_mock = Mock()
        self.spark_mock.sql.return_value = Mock()
        self.spark_mock.sql.return_value.dtypes = [('col1', 'bigint'), ('col2', 'int')]
        self.spark_mock.sql.return_value.columns = ['col1', 'col2']
        self.spark_mock.sql.return_value.count.return_value = 1
        self.spark_mock.sql.return_value.sort.return_value = self.spark_mock.sql.return_value
        self.spark_mock.sql.return_value.subtract.return_value = self.spark_mock.sql.return_value
        self.spark_mock.sql.return_value.isEmpty.return_value = True
        self.spark_mock.sql.return_value.write.return_value = self.spark_mock.sql.return_value

    def test_compare_metrics_success(self):
        # Mock successful compare_metrics scenario
        with patch('builtins.print') as mock_print:
            result = compare_metrics(self.spark_mock, "table1", "table2", "filter", "dim_cols", "dim_metrics_cols", "ignored_cols")
        
        self.assertIsInstance(result, str)
        self.assertIn("Successfully stored", result)
        mock_print.assert_called_once_with("Metrics comparison successful for table1 and table2.")

    def test_compare_metrics_empty_result(self):
        # Mock compare_metrics with empty result scenario
        self.spark_mock.sql.return_value.isEmpty.return_value = True
        
        with patch('builtins.print') as mock_print:
            result = compare_metrics(self.spark_mock, "table1", "table2", "filter", "dim_cols", "dim_metrics_cols", "ignored_cols")
        
        self.assertIsInstance(result, str)
        self.assertIn("No data to compare", result)
        mock_print.assert_called_once_with("No data found to compare metrics for table1 and table2.")

    def test_compare_metrics_exception(self):
        # Mock compare_metrics with exception scenario
        self.spark_mock.sql.return_value.isEmpty.side_effect = Exception("Test exception")

        with patch('builtins.print') as mock_print:
            result = compare_metrics(self.spark_mock, "table1", "table2", "filter", "dim_cols", "dim_metrics_cols", "ignored_cols")
        
        self.assertIsInstance(result, str)
        self.assertIn("Error occurred", result)
        mock_print.assert_any_call("Error occurred while comparing metrics for table1 and table2:")
        mock_print.assert_any_call("Test exception")

    def test_compare_metrics_with_validation_error(self):
        spark_mock = Mock(spec=self.spark)
        spark_mock.sql.return_value = Mock()
        spark_mock.sql.return_value.dtypes = [('col1', 'string')]
        spark_mock.sql.return_value.columns = ['col1']
        spark_mock.sql.return_value.count.return_value = 1

        # Test compare_metrics function with ValidationError
        with self.assertRaises(ValidationError):
            compare_metrics(spark_mock, "table1", "table2", "filter")

if __name__ == "__main__":
    unittest.main()
