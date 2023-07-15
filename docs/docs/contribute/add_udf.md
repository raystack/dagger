# ADD UDF

Want a function to use in SQL which is not supported both by Flink and one of the pre-supported udfs? You can simply write your User-Defined function and contribute to the dagger. Read more on how to use UDFs [here](../guides/use_udf.md).

`Note`: _Please go through the [Contribution guide](../contribute/contribution.md) to know about all the conventions and practices we tend to follow and to know about the contribution process to the dagger._

For adding custom UDFs follow these steps:

- Ensure none of the [built-in functions](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/functions/systemfunctions/) or [existing UDF](../reference/udfs.md) suits your requirement.

- For adding a UDF, figure out which type of UDF you required. Flink supports three types of [User defined function](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/functions/udfs/). Choose one of them according to the requirement.

- There are options for programming language you can choose for adding UDF, which is using Java, Scala and Python.

- For adding UDF with Java/Scala:

  - Follow [this](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/functions/udfs/) for more insights on writing your UDF.
  - UDF needs to be added in the `function-type` folder inside [this](https://github.com/raystack/dagger/tree/main/dagger-functions/src/main/java/org/raystack/dagger/functions/udfs) on `dagger-functions` subproject.
  - Extend either of [ScalarUdf](https://github.com/raystack/dagger/blob/main/dagger-common/src/main/java/org/raystack/dagger/common/udfs/ScalarUdf.java), [TableUdf](https://github.com/raystack/dagger/blob/main/dagger-common/src/main/java/org/raystack/dagger/common/udfs/TableUdf.java) or [AggregateUdf](https://github.com/raystack/dagger/blob/main/dagger-common/src/main/java/org/raystack/dagger/common/udfs/AggregateUdf.java) from `dagger-common`. They are boilerplate contracts extending Flink UDF classes. These classes do some more preprocessing(like exposing some metrics) in the `open` method behind the scene.
  - Register the UDF in [this](https://github.com/raystack/dagger/blob/main/dagger-functions/src/main/java/org/raystack/dagger/functions/udfs/factories/FunctionFactory.java) class. This is required to let Flink know about your function.
  - If you have some business-specific use-cases and you don't want to add UDFs to the open-sourced repo, you can have a separate local codebase for those UDFs. Those UDFs need to be registered in a similar class like the [`UDFFactory`](https://github.com/raystack/dagger/blob/main/dagger-common/src/main/java/org/raystack/dagger/common/udfs/UdfFactory.java). Keep both the UDF classes and the factory class in the classpath of Dagger. Configure the fully qualified Factory class in the `FUNCTION_FACTORY_CLASSES` parameter and you will be able to use the desired UDF in your query.

- For adding UDF with Python:
  - Follow [this](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table/udfs/overview/) for more insights on writing your UDF.
  - UDF need to be added inside [this](https://github.com/raystack/dagger/tree/main/dagger-py-functions/udfs) on `dagger-py-functions` directory.
  - Ensure that the filename and method name on the python functions is the same. This name will be registered by dagger as a function name which later can be used on the query.
  - Ensure to add dependency needed for the python function on the [requirements.txt](https://github.com/raystack/dagger/tree/main/dagger-py-functions/requirements.txt) file.
  - Add python unit test and the make sure the test is passed.
  - If you have some business-specific use-cases and you don't want to add UDFs to the open-sourced repo, you can have a separate local codebase for those UDFs and specify that file on the python configuration.
- Bump up the version and raise a PR for the same. Also please add the registered function to the [list of udfs doc](../reference/udfs.md).

In the subsequent release of the dagger, your functions should be useable in the query.
