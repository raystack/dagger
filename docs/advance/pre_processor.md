# Introduction
Pre processors enable the users to add Flink operators/transformations before passing on the stream to the SQL query. Each stream registered on daggers can have chained processors. They will run and transform the table before registering it.

## Type of Preprocessors
Currently, there is only one type of pre-processor. 
* [Transformers](docs/../../guides/use_transformer.md)


<p align="center">
  <img src="../assets/pre-processor.png" />
</p>