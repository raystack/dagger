# Use UDF

## Explore Flink Supported Functions

Queries in Dagger are similar to standard ANSI SQL with some additional syntax. So many standard SQL supported functions are also supported by Flink hence available to dagger out of the box.

To check if your desired function is supported by Flink follow these steps :

- Dagger uses Apache Calcite for Query evaluation. You can use Calcite supported functions in Dagger with the exceptions of some. So first check the calcite supported functions [here](https://calcite.apache.org/docs/reference.html). Try to use them in a Dagger query to check if Dagger supports them.
- Flink also supports some generic functions as Built-in Functions. You can check them out [flink-udfs](https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/table/functions/systemfunctions/). You can use them directly.
- If Calcite and Flink do not support your desired function, try exploring generic pre-existing custom User Defined Functions (UDFs) developed by us which are listed in the next section.

## Explore Custom UDFs

Some of the use-cases can not be solved using Flink SQL & the Apache Calcite functions. In such a scenario, Dagger can be extended to meet the requirements using User Defined Functions (UDFs). UDFs can be broadly classified into the following categories:

- ### Scalar Functions

  Maps zero or more values to a new value. These functions are invoked for each data in the stream.

- ### Aggregate Functions

  Aggregates one or more rows, each with one or more columns to a value. Aggregates data per dimension. int DistinctCount(int metric) // calculates distinct count of a metric in a given window. Eg: DistinctCount(driver_id) will return unique driver IDs in a window.

- ### Table Functions

  Maps zero or more values to multiple rows and each row may have multiple columns.

All the supported java udfs present in the `dagger-functions` subproject in [this](https://github.com/goto/dagger/tree/main/dagger-functions/src/main/java/com/gotocompany/dagger/functions/udfs) directory.

All the supported python udfs present in the [dagger-py-functions](https://github.com/goto/dagger/tree/main/dagger-py-functions/udfs/) directory.

Follow [this](../reference/udfs.md) to find more details about the already supported UDFs in the dagger.

If any of the predefined functions do not meet your requirement you can create your custom UDFs by extending some implementation. Follow [this](../contribute/add_udf.md) to add your custom UDFs in the dagger.

## Python Environment Setup

Python UDF execution requires Python version (3.6, 3.7 or 3.8) with PyFlink installed.

PyFlink is available in PyPi and can be installed as follows:
```
$ python -m pip install apache-flink==1.14.3
```

To satisfy the PyFlink requirement regarding the Python environment version, you need to soft link python to point to your python3 interpreter:
```
ln -s /usr/bin/python3 python
```
