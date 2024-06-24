# Mops data-validation-framework library
## Summary
The project is library of a Python module. The '**data-validation-framework**' library is designed to facilitate count data and row data and metric data validation for Hive tables specified in a YAML configuration file as input parameters to the library function **get_multiple_table_data_validation_details()**. It offers functionalities to compare and validate data between two tables based on input parameters provided by users such as table1, table2, filter, etc.

- If you are a developer who would like to use this library for testing or in your databricks workflow read [How To use the data-validation-framework library:](https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework/-/blob/feature/validation_framework/README.md#how-to-use-the-data-validation-framework-library) 


## Library Installation:
```bash
pip install data-validation-framework --index-url https://artifactory.zgtools.net/artifactory/api/pypi/pypi-local/simple --verbose --extra-index-url https://pypi.org/simple/

```

# How To use the data-validation-framework library:


[[_TOC_]]

The data-validation-framework consist a function called **get_multiple_table_data_validation_details()** which task yaml file path string as input parameter. In a YAML configuration user need to specify the input parameters for their tables in order to process validate multiple tables added in the yaml. Please Refer this section for [input parmaeter details](https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework/-/blob/feature/validation_framework/README.md#input-parameters-details-in-yaml-file).

The source code for the library functions is maintained in the data-validation-framework repo. This library can be used in databricks notebooks and workflows.

## Library Functions:
### get_multiple_table_data_validation_details:
  **get_multiple_table_data_validation_details(*path: string = ‘YAML File Path String’*)**

### Purpose: 
**get_multiple_table_data_validation_details()** is a util function to process a Count Comparison, Row(data) Comparison and Metric validation based on the input YAML configuration provided by the user. This function accepts a YAML file path as a string input from the user. It then processes validations for input tables based on the parameters provided by the user within the YAML file.

This function can be used in 2 types of tasks:
  1. Notebook Task: This function can be integrated into a workflow as a notebook task or executed manually using notebook. It facilitates table testing by providing functionalities for comprehensive data validation.
  2. Python Wheel Task: This task involves packaging the code into a Python wheel, which can be distributed and used in various environments for performing data validation tasks efficiently.

**Note**: *It’s recommended to use as Notebook task if you wish to use this in workflow.*

Within the above function we are processing primarily 4 functions in order to do overall Count Comparison, Row(data) Comparison and Metric validation.

- For Count validation we are processing *compare_counts()*. This function compares and returns the row count between two tables based applying the filter(if added) and difference between them. 

- For Row data validation we are processing: *compare_columns()* and *compare_rows()*. The *compare_columns()* checks if the columns of two tables are the same, if those are same then execute *compare_rows()* to perform row-level(row data) validation between two tables using our MD5 hashing approach. This function return the data difference count details between 2 tables.

- Metric validation we are processing *compare_metrics()* function which returns a seperate dataframe containg the metric validation details and output is stored as table to the 'sandbox_marketing.mart_mops_tests'.

- **Note**: The metric validation function is processed only when **metric_validation_active** is set to **True** and code identifies that table1 and table2 data is not matching for the tables for that row input. If data match found in *compare_rows()* for those tables then there is no need to process *compare_metric_sum()* function. 

## Input Parameters Details in YAML File:
Below are the parameters that user needs to provide in the yaml. Alternatively, users can create their own YAML file adhering strictly to below input parameters names and pass that YAML file path string to the function like below  
```bash
  get_multiple_table_data_validation_details(path=’/Workspace/path/to/your/yamlfile/yaml.yml’)
```

The below parameters should be specified under ‘**rows**’ section(do not change param’s names) in the yaml. (Examples file: [tables.yml](https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#files/1760665646812858) )

1. **is_validation_active** (Bool: Required): 
    - True or False boolean value. The default value is set to True for this parma but If set False by user then the function will skip the overall validation process for its tables.
    - Users can set it to False in yaml if they do not wish to validate specific tables or already checked tables during validation. 

2. **table1** (str: Required): Name of the first hive/databricks table in format catalog.schema.table_name. 

3. **table2** (str: Required): Name of the first hive/databricks table in format catalog.schema.table_name.

4. **filter_condition**: (str: optional):  
    - Filter condition that will be applied on both table1 and table2. Employ as in sql after  WHERE clause or keep empty string " " if you do not wish to add. E.g. *"p_data_date = date'2024-05-01' and brand= 'zillow'"*

5. **materialization** (str: optional): Type of table. Add a string e.g incremental/full_load.

6. **metric_validation_status**: (Bool: optional): 
    - True or False boolean value. Default value is set to False but If set True then the function will process the metric data validation when there data mismatch in table1 and table2
    - **Note: This param will process the metric validation (if set True) when there is data mismatch between table1 and table2 else it will skip metric validation even if it's True since there is no need to do this operation if data is matching table1 and table2.**

7. **dimenssion_columns** (str: optional): 
  Specify the dimension or slice columns(common in both tables) you wish to include in the validation query using this parameter. Provide it in string quotes, If you’re including multiple columns, list them in string format separated by commas. Eg.
    - Default value is set to empty string (“ ”)
    - For single column use format:  ‘column_name’ 
    - For multiple column, list them as: ‘column1, column2, column3, column4’

8. **dim_metric_columns** (str: optional): 
  Provide the dimension columns (common in both tables) to be converted as metric columns (perform count agg) in the validation query. 
    - Default value is set to empty string (“ ”)
    - For single column eg:  ‘column_name’ 
    - For multiple column e.g: ‘column1, column2, column3, column4’

9. **ignored_columns** (str: optional): 
  Provide the list of columns that you wish to ignore/exclude from the metrics of integral type e.g ‘zuid’, ‘account_id’, ‘campaign_id’, ‘process_year’ etc. 
    - Default value is set to empty string (“ ”)
    - For single column eg:  ‘column_name’ 
    - For multiple column e.g: ‘column1, column2, column3, column4’

### Example configuration (Also refer tables.yml):

```bash
rows:
  - is_validation_active: True
    table1: 'hive_metastore.mart_mops.real_time_touring_agent'
    table2: 'sandbox_marketing.mart_mops_test.real_time_touring_agent_managed'
    filter_condition: "p_data_date >= date'2024-05-16'"
    materialization: 'incremental'
    metric_validation_active: True
    ignored_columns: 'mls_id, team_lead'
    dimenssion_columns: 'p_data_date'
    dim_metrics_columns: 'team_lead, mls_id'
```

## Steps to use **get_multiple_table_data_validation_details()**:
Below are the steps to use the library in the databricks notebooks. Please check this example data validation framework testing notebook for the usage of **get_multiple_table_data_validation_details()** https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#notebook/3501886217257531/command/2038113280157046

Perform below command for to instal package in the databricks notebookl: 
### 1. Install Package:
```bash
!pip install data-validation-framework --index-url https://artifactory.zgtools.net/artifactory/api/pypi/pypi-local/simple --verbose --extra-index-url https://pypi.org/simple/
```
### 2. Import library-functions and pyspark functions:
```bash
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from data_validation_framework.process_validation_task import get_multiple_table_data_validation_details
```
### 3. Input YAML path:
Create a yaml file (like tables.yml https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#files/176066564681285) somewhere in databricks workspace or in the git repo (If you wish to create this as a task in workflows) with the above mentioned required input parameters for the tables you are comparing and define path variable in notebook providing the yaml file path.
```bash
path = '/Workspace/Users/v-tushara@zillowgroup.com/tables.yml'
```

### 4. Execute get_multiple_table_data_validation_details(path):
Pass the ‘path’ variable to the functions get_multiple_table_data_validation_details() and execute. Once executed it will process validation and create a dataframe containing details such as table1 count, table2 count, count difference between table1 and table2, difference percentage, count match status, table1_subtract_cnt(count of data rows in table1 but not present in table2. The .subtract action in pyspark), table2_subtract_cnt (count of data rows in table2 but not present in table1. The .subtract() action in pyspark), table1_datamatch_st (Whether table1 data matched with table2 True or False), table2_datamatch_st (Whether table2 data matched with table2, True or False ) and partition column details. 

You can view the dataframe by df.show() or display(df).

Prerequisite:
  - Before you execute the function make sure that there is no difference in schema of table1 and table2. If there is column mismatch between table1 and table2 exception will be raised.

```bash
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.process_validation_task import  get_multiple_table_data_validation_details

path = '/Workspace/Users/v-tushara@zillowgroup.com/tables.yml'

df = get_multiple_table_data_validation_details(path)
display(df)
```

### Example Validation Notebook():
Refer to this example notebook showcasing how to use the get_multiple_table_data_validation_details() function, leveraging the input tables specified in the [tables.yml](https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#files/1760665646812858)

notebook: https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#notebook/3501886217257531/command/2038113280157046


## Use library with Databrick's Notebook task and Python Wheel task:

The **data-validation-framework** library can be utilized in the databricks workflows using Notebook Task and Python Task. We can install the library in the databricks cluster and use any of these tasks as per below mentioned steps.

Prerequisites:
  - Go to the *Libraries* section inside the Databricks Cluster.  
  - Install ‘data-validation-framework’ python library selecting *PyPI* Library source 
  - Provide this *Index URL: https://artifactory.zgtools.net/artifactory/api/pypi/zg-pypi/simple* when installing the and make sure the library is installed successfully in the cluster.

### Notebook Task(recommended):
  1. Create a notebook, add the code for library execution as mentioned in the step [Execute get_multiple_table_data_validation_details(path)](https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework/-/blob/feature/validation_framework/README.md#4-execute-get_multiple_table_data_validation_detailspath) and replace the path variable value with your yaml file path created as per the [configurations](https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework/-/blob/feature/validation_framework/USAGE.md#input-parameters-details-in-yaml-file) for your tables.
  2. Add **data-validation-framework** library dependency providing the *index url* in the task’s configuration in your databricks workflow and execute the task.


### Python Wheel Task:
- To execute the validation process using the python wheel task, we have created this ‘get_data_validations_wheel_task’ entry point.
- This entry point function accepts takes Yaml file path string as an argument and processes the validation execution.
- Use this entry point python wheel task and package name **data_validation_framework** to execute.
    1. Add **get_data_validations_wheel_task** in the ***Entry Point*** section and **data_validation_framework** in ***Package Name*** section in the python wheel task
    2. Provide the yaml file path variable in the ***Parameter*** section for the entry point function selecting using 'Keyword argument' and execute the task.

### Sample Workflow
Refer this sample workflow created. Using the library with Python wheel and Notebook task:
[Validation Framework Utility Test Example](https://zg-marketing-lab.cloud.databricks.com/jobs/144060734374961/tasks?o=3559368047888194) 
