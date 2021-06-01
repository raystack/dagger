## Dagger Lifecycle

Architecturally after the creation of Dagger, it goes through several stages before materializing the results to an output stream.

- In the first stage, Dagger sets up the corresponding configurations and the input Kafka stream(s) from which it has to consume Data. The Deserialization of proto Data from the input Kafka topic is done in this stage.
- Before executing the streaming SQL, Dagger undergoes a Pre-processor stage. Pre-processor is similar to post processors and currently support only transformers. They can be used to do some filtration of data or to do some basic processing before SQL.
- In the third stage, it has to register all the custom UDFs. After that, it applies the SQL on the input streams and produces a resultant stream. But in the case of complex business requirements, SQL is not just enough to handle the complexity of the problem. So there comes the Post Processors.
- In the fourth stage or Post Processors stage, the output of the previous stage is taken as input and some complex transformation logic can be applied on top of it to produce the result ultimately. There can be three types of Post Processors: external, internal, and transformers. You can find more on post processors and their responsibilities here.
- In the final stage or sink stage, the final output is serialized accordingly and sent to a downstream (which can either be Kafka or InfluxDB) for further use.
