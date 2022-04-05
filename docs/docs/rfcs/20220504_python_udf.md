## Motivation
Dagger users include developers, analysts, data scientists, etc. For users to use Dagger, they can add new capabilities by defining their own functions commonly referred to as UDFs. Currently, Dagger only supports java as the language for the UDFs. To democratize the process of creating, maintaining the UDFs we want to add support for python.

## Requirement
Support for adding Python UDF on Dagger
End to end flow on adding and using Python UDF on Dagger. 


## Python User Defined Function
There are two kinds of Python UDF that can be registered on Dagger:
* General Python UDF
* Vectorized Python UDF


It shares the similar way as the general user-defined functions on how to define vectorized user-defined functions. Users only need to add an extra parameter func_type="pandas" in the decorator udf or udaf to mark it as a vectorized user-defined function.



Type | General Python UDF | Vectorized Python UDF 
--- | --- | --- |
Data Processing Method | One piece of data is processed each time a UDF is called | Multiple pieces of data are processed each time a UDF is called
Serialization/Deserialization | Serialization and Deserialization are required for each piece of data on the Java side and Python side| The data transmission format between Java and Python is based on Apache Arrow: <ul><li> Pandas supports Apache Arrow natively, so serialization and deserialization are not required on Python side</li><li>On the Java side, vectorized optimization is possible, and serialization/deserialization can be avoided as much as possible</li></ul>|
|Exection Performance|Poor|Good<ul><li>Vectorized execution is of high efficiency</li><li>High-performance python UDF can be implemented based on high performance libraries such as pandas and numpy</li></ul>

Note: 

When using vectorized udf, Flink will convert the messages to pandas.series, and the udf will use that as an input and the output also pandas.series. The pandas.series size for an input and output should be the same.

## Configuration 
There are a few configuration that required for using python UDF, and also options that we can adjust for optimization.

Configuration that will be added on Dagger codebase:
| Key | Default | Type | Example
| --- | ---     | ---  | ----  |
|python.archives|(none)|String||
|python.client.executable|"python"|String||
|python.executable|"python"|String||
|python.files|(none)|String||
|python.fn-execution.arrow.batch.size|10000|Integer||
|python.fn-execution.bundle.size|100000|Integer||
|python.fn-execution.bundle.time|1000|Long||
|python.fn-execution.memory.managed|TRUE|Boolean||
|python.map-state.iterate-response-batch-size|1000|Integer||
|python.map-state.read-cache-size|1000|Integer||
|python.map-state.write-cache-size|1000|Integer||
|python.state.cache-size|1000|Integer||
|python.metric.enabled|TRUE|Boolean||
|python.profile.enabled|FALSE|Boolean||
|python.requirements|(none)|String||


The config `python.operator-chaining.enabled` is not required because this config can only be used on Python Datastream API.

