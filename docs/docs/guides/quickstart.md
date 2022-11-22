# Dagger Quickstart

There are 2 ways to set up and get dagger running in your machine in no time - 
1. **[Docker Compose Setup](quickstart.md#docker-compose-setup)** - recommended for beginners
2. **[Local Installation Setup](quickstart.md#local-installation-setup)** - for more advanced usecases

## Docker Compose Setup

### Prerequisites

1. **You must have docker installed**

Following are the steps for setting up dagger in docker compose -
1. Clone Dagger repository into your local

   ```shell
   git clone https://github.com/odpf/dagger.git
   ```
2. cd into the docker-compose directory:
   ```shell
   cd dagger/quickstart/docker-compose 
   ```
3. fire this command to spin up the docker compose:
   ```shell
   docker compose up 
   ```
This will spin up docker containers for the kafka, zookeeper, stencil, kafka-producer and the dagger.
4. fire this command to gracefully stop all the docker containers. This will save the container state and help to speed up the setup next time. All the kafka records and topics will also be saved  :
   ```shell
   docker compose stop 
   ```
   To start the containers from their saved state run this command
   ```shell
   docker compose start 
   ```
5. fire this command to gracefully remove all the containers. This will delete all the kafka topics/ saved data as well:
   ```shell
   docker compose down 
   ```
   
### Workflow

Following are the containers that are created, in chronological order, when you run `docker compose up`  - 

1. **Zookeeper** -  Container for the Zookeeper service is created and listening on port 2187. Zookeeper is a service required by the Kafka server.
2. **Kafka** - Container for Kafka server is created and is exposed on port 29094. This will serve as the input data source for the Dagger.
3. **init-kafka** - This container creates the kafka topic `dagger-test-topic-v1` from which the dagger will pull the Kafka messages.
4. **Stencil** - It compiles the proto file and creates a proto descriptor. Also it sets up an http server serving the proto descriptors required by dagger to parse the Kafka messages. 
5. **kafka-producer** - It runs a script to generate the random kafka messages and sends one message to the kafka topic every second.
6. **Dagger** - Clones the Dagger Github repository and builds the jar. Then it creates an in-memory flink cluster and uploads the dagger job jar and starts the job.

The dagger runs a simple aggregation query which will count the number of bookings , i.e. kafka messages, in every 30 seconds interval. The output will be visible in the logs in the terminal itself. You can edit this query (`FLINK_SQL_QUERY` variable) in the `local.properties` file inside the `quickstart/docker-compose/resources` directory.

## Local Installation Setup

### Prerequisites

1. **Your Java version is Java 8**: Dagger as of now works only with Java 8. Some features might not work with older or later versions.
2. Your **Kafka** version is **3.0.0** or a minor version of it
3. You have **kcat** installed: We will use kcat to push messages to Kafka from the CLI. You can follow the installation steps [here](https://github.com/edenhill/kcat). Ensure the version you install is 1.7.0 or a minor version of it.
4. You have **protobuf** installed: We will use protobuf to push messages encoded in protobuf format to Kafka topic. You can follow the installation steps for MacOS [here](https://formulae.brew.sh/formula/protobuf). For other OS, please download the corresponding release from [here](https://github.com/protocolbuffers/protobuf/releases). Please note, this quickstart has been written to work with[ 3.17.3](https://github.com/protocolbuffers/protobuf/releases/tag/v3.17.3) of protobuf. Compatibility with other versions is unknown.
5. You have **Python 2.7+** and **simple-http-server** installed: We will use Python along with simple-http-server to spin up a mock Stencil server which can serve the proto descriptors to Dagger. To install **simple-http-server**, please follow these [installation steps](https://pypi.org/project/simple-http-server/).

### Quickstart

1. Clone Dagger repository into your local

```shell
git clone https://github.com/odpf/dagger.git
```
2. Next, we will generate our proto descriptor set. Ensure you are at the top level directory(`dagger`) and then fire this command

```
./gradlew clean dagger-common:generateTestProto
```

This command will generate a descriptor set containing the proto descriptors of all the proto files present under `dagger-common/src/test/proto`. After running this, you should see a binary file called `dagger-descriptors.bin` under `dagger-common/src/generated-sources/descriptors/`.

3. Next, we will setup a mock Stencil server to serve this proto descriptor set to Dagger. Open up a new tab in your terminal and `cd` into this directory: `dagger-common/src/generated-sources/descriptors`. Then fire this command:

```python
python -m SimpleHTTPServer 8000
```

This will spin up a mock HTTP server and serve the descriptor set we just generated in the previous step at port 8000.
The Stencil client being used in Dagger will fetch it by calling this URL. This has been already configured in `local.properties`, as we have set `SCHEMA_REGISTRY_STENCIL_ENABLE` to true and pointed `SCHEMA_REGISTRY_STENCIL_URLS` to `http://127.0.0.1:8000/dagger-descriptors.bin`.

4. Next, we will generate and send some messages to a sample kafka topic as per some proto schema. Note that, in `local.properties` we have set `INPUT_SCHEMA_PROTO_CLASS` under `STREAMS` to use `io.odpf.dagger.consumer.TestPrimitiveMessage` proto. Hence, we will push messages which conform to this schema into the topic. For doing this, please follow these steps:
   1. `cd` into the directory `dagger-common/src/test/proto`. You should see a text file `sample_message.txt` which contains just one message. We will encode it into a binary in protobuf format.
   2. Fire this command:
   ```protobuf
   protoc --proto_path=./ --encode=io.odpf.dagger.consumer.TestPrimitiveMessage ./TestLogMessage.proto < ./sample_message.txt > out.bin
   ```
   This will generate a binary file called `out.bin`. It contains the binary encoded message of `sample_message.txt`.

   3. Next, we will push this encoded message to the source Kafka topic as mentioned under `SOURCE_KAFKA_TOPIC_NAMES` inside `STREAMS` inside `local.properties`. Ensure Kafka is running at `localhost:9092` and then, fire this command:
   ```shell
   kcat -P -b localhost:9092 -D "\n" -T -t dagger-test-topic-v1 out.bin
   ```
   You can also fire this command multiple times, if you want multiple messages to be sent into the topic. Just make sure you increment the `event_timestamp` value every time inside `sample_message.txt` and then repeat the above steps. 
6. `cd` into the repository root again (`dagger`) and start Dagger by running the following command:
```shell
./gradlew dagger-core:runFlink
```

After some initialization logs, you should see the output of the SQL query getting printed.

### Troubleshooting

1. **I am pushing messages to the kafka topic but not seeing any output in the logs.** 

   This can happen for the following reasons:

   a. Pushed messages are not reaching the right topic: Check for any exceptions or errors when pushing messages to the Kafka topic. Ensure that the topic to which you are pushing messages is the same one for which you have configured Dagger to read from under `STREAMS` -> `SOURCE_KAFKA_TOPIC_NAMES` in `local.properties`

   b. The consumer group is not updated: Dagger might have already processed those messages. If you have made any changes to the setup, make sure you update the `STREAMS` -> `SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID` variable in `local.properties` to some new value.

2. **I see an exception `java.lang.RuntimeException: Unable to retrieve any partitions with KafkaTopicsDescriptor: Topic Regex Pattern`**

   This can happen if the topic configured under `STREAMS` -> `SOURCE_KAFKA_TOPIC_NAMES` in `local.properties` is new and you have not pushed any messages to it yet. Ensure that you have pushed atleast one message to the topic before you start dagger.
