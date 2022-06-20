## Motivation
Dagger users include developers, analysts, data scientists, etc. For users to use Dagger, they can add new capabilities by defining their own functions commonly referred to as UDFs. Currently, Dagger only supports java as the language for the UDFs. To democratize the process of creating and maintaining the UDFs we want to add support for python.

## Requirement
Support for adding Python UDF on Dagger
End-to-end flow on adding and using Python UDF on Dagger. 


## Python User Defined Function
There are two kinds of Python UDF that can be registered on Dagger:
* General Python UDF
* Vectorized Python UDF


It shares a similar way as the general user-defined functions on how to define vectorized user-defined functions. Users only need to add an extra parameter func_type="pandas" in the decorator udf or udaf to mark it as a vectorized user-defined function.


Type | General Python UDF | Vectorized Python UDF 
--- | --- | --- |
Data Processing Method | One piece of data is processed each time a UDF is called | Multiple pieces of data are processed each time a UDF is called
Serialization/Deserialization | Serialization and Deserialization are required for each piece of data on the Java side and Python side| The data transmission format between Java and Python is based on Apache Arrow: <ul><li> Pandas supports Apache Arrow natively, so serialization and deserialization are not required on Python side</li><li>On the Java side, vectorized optimization is possible, and serialization/deserialization can be avoided as much as possible</li></ul>|
|Exection Performance|Poor|Good<ul><li>Vectorized execution is of high efficiency</li><li>High-performance python UDF can be implemented based on high performance libraries such as pandas and numpy</li></ul>

Note: 

When using vectorized udf, Flink will convert the messages to pandas.series, and the udf will use that as an input and the output also pandas.series. The pandas.series size for input and output should be the same.

## Configuration 
There are a few configurations that required for using python UDF, and also options we can adjust for optimization.

Configuration that will be added on Dagger codebase:
| Key | Default | Type | Example
| --- | ---     | ---  | ----  |
|PYTHON_UDF_ENABLE|false|Boolean|false|
|PYTHON_UDF_CONFIG|(none)|String|{"PYTHON_FILES":"/path/to/files.zip", "PYTHON_REQUIREMENTS": "requirements.txt", "PYTHON_FN_EXECUTION_BUNDLE_SIZE": "1000"}|

The following variables than can be configurable on `PYTHON_UDF_CONFIG`:
| Key | Default | Type | Example
| --- | ---     | ---  | ----  |
|PYTHON_ARCHIVES|(none)|String|/path/to/data.zip|
|PYTHON_FILES|(none)|String|/path/to/files.zip|
|PYTHON_REQUIREMENTS|(none)|String|/path/to/requirements.txt|
|PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE|10000|Integer|10000|
|PYTHON_FN_EXECUTION_BUNDLE_SIZE|100000|Integer|100000|
|PYTHON_FN_EXECUTION_BUNDLE_TIME|1000|Long|1000|


## Registering the Udf
Dagger will automatically register the python udf as long as the files meets the following criteria:
* Python file names should be the same with its function method
  Example:
  
  sample.py
  ```
  from pyflink.table import DataTypes
  from pyflink.table.udf import udf
  
  
  @udf(result_type=DataTypes.STRING())
  def sample(word: str):
      return word + "_test"
  ```
* Avoid adding duplicate `.py` filenames. e.g: `__init__.py`


## Release the Udf
List of udfs for dagger, will be added on directory `dagger-py-functions` include with its test, data files that are used on the udf, and the udf dependency(requirements.txt).
All of these files will be bundled to single zip file and uploaded to assets on release.

## Reference
[Flink Python User Defined Functions](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table/udfs/overview/)
