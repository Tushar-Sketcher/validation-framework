import unittest
from unittest.mock import patch, MagicMock, mock_open
from data_validation_framework.operations import validation_process
from data_validation_framework.common import get_dataframe
from data_validation_framework.process_validations_task import (
    get_data_validation_details,
    get_multiple_table_data_validation_details
)


# Mocking the spark session object
class MockSparkSession:
    def __init__(self):
        pass

@patch('data_validation_framework.operations.validation_process')
@patch('data_validation_framework.common.get_dataframe')
class TestDataValidation(unittest.TestCase):

    def test_get_data_validation_details_success(self, mock_get_dataframe, mock_validation_process):
        # Mock successful return value for validation_process and get_dataframe
        mock_validation_process.return_value = [{'table': 'table1', 'count': 100}]
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Call the function with mock arguments
        result = get_data_validation_details(
            MagicMock(spec=MockSparkSession),mock_validation_process,mock_get_dataframe,'table1','table2','',True,'','','',True,None
        )

        # Assertions
        mock_validation_process.assert_called_once_with(
            MagicMock(spec=MockSparkSession),[],'table1','table2','',True,'','','',True,None
        )
        mock_get_dataframe.assert_called_once()
        self.assertIsInstance(result, MagicMock)

    def test_get_data_validation_details_validation_inactive(self, mock_get_dataframe, mock_validation_process):
        # Mock return value for validation_process and get_dataframe
        mock_validation_process.return_value = [{'table': 'table1', 'count': 100}]
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Call the function with validation_inactive = False
        result = get_data_validation_details(
            MagicMock(spec=MockSparkSession),mock_validation_process,mock_get_dataframe,'table1','table2','',False,'','','',True,None
        )

        # Assertions
        mock_validation_process.assert_not_called()  # Ensure validation_process is not called
        mock_get_dataframe.assert_called_once()
        self.assertIsInstance(result, MagicMock)

    def test_get_data_validation_details_exception_handling(self, mock_get_dataframe, mock_validation_process):
        # Mock exception in validation_process
        mock_validation_process.side_effect = Exception("Validation error")
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Call the function with mock arguments
        result = get_data_validation_details(
            MagicMock(spec=MockSparkSession),mock_validation_process,mock_get_dataframe,'table1','table2','',True,'','','',True,None
        )

        # Assertions
        mock_validation_process.assert_called_once()
        mock_get_dataframe.assert_called_once()
        self.assertIsInstance(result, MagicMock)

    def test_get_multiple_table_data_validation_details(self, mock_get_dataframe, mock_validation_process):
        # Mock return value for validation_process and get_dataframe
        mock_validation_process.return_value = [{'table': 'tableA', 'count': 200}, {'table': 'tableB', 'count': 300}]
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Mock yaml content and path
        yaml_content = """
        rows:
          - table1: tableA
            table2: tableB
            filter_condition: ''
            is_validation_active: true
            metric_validation_active: false
            dimenssion_columns: ''
            dim_metrics_columns: ''
            ignored_columns: ''
            materialization: null
        """
        with patch('builtins.open', mock_open(read_data=yaml_content)):
            result = get_multiple_table_data_validation_details('dummy_path.yaml')

        # Assertions
        mock_validation_process.assert_any_call(
            MagicMock(spec=MockSparkSession),[],'tableA','tableB','',True,'','','',True,None
        )
        mock_get_dataframe.assert_called_once()
        self.assertIsInstance(result, MagicMock)  # Adjust to match actual return type

    def test_get_multiple_table_data_validation_details_invalid_yaml(self, mock_get_dataframe, mock_validation_process):
        # Mock return value for validation_process and get_dataframe
        mock_validation_process.return_value = [{'table': 'tableA', 'count': 200}, {'table': 'tableB', 'count': 300}]
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Mock invalid yaml content and path
        invalid_yaml_content = "invalid yaml"
        with patch('builtins.open', mock_open(read_data=invalid_yaml_content)):
            result = get_multiple_table_data_validation_details('dummy_path.yaml')

        # Assertions
        mock_validation_process.assert_not_called()  # Ensure validation_process is not called
        mock_get_dataframe.assert_not_called()  # Ensure get_dataframe is not called
        self.assertIsNone(result)  # Adjust to match actual behavior

if __name__ == "__main__":
    unittest.main()
