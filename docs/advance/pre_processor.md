# Introduction
Each stream registered on daggers can have chained processors. They will run and transform the table before registering it for SQL. 

## Type of Preprocessors
Currently, there is only one type of pre-processor. 
* Transformers

### Transformers
Transformers preprocessors are custom code that users can specify in the configuration. A user can write any transformation on the datastream like a filter, map etc. To add a transformer, the user needs to implement io.odpf.dagger.common.core.Transformer and provide the configuration during runtime. Transformers are defined per table and each table can have chained transformers.

<p align="center">
  <img src="../assets/pre-processor.png" />
</p>