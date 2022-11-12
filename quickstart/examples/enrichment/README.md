## Enrichment using Post Processors
External Post Processor is the one that connects to an external data source to fetch data in an async manner and perform enrichment of the stream message. 

In this example we use Elasticsearch as the external source.
This allows you to enrich your data stream with the data on any remote [Elasticsearch](https://www.elastic.co/). For example, let's say you have payment transaction logs in the input stream but user profile information in Elasticsearch, then you can use this post processor to get the profile information in each record.

#### Sample Configuration

```properties
PROCESSOR_POSTPROCESSOR_ENABLE = true
PROCESSOR_POSTPROCESSOR_CONFIG = {
  "external_source": {
    "es": [
      {
        "host": "127.0.0.1",
        "port": "9200",
        "endpoint_pattern": "/customers/customer/%s",
        "endpoint_variables": "customer_id",
        "retry_timeout": "5000",
        "socket_timeout": "6000",
        "stream_timeout": "5000",
        "connect_timeout": "5000",
        "capacity": "30",
        "output_mapping": {
          "customer_profile": {
            "path": "$._source"
          }
        }
      }
    ]
  }
}
```


## Docker Compose Setup

### Prerequisites

1. **You must have docker installed**

Following are the steps for setting up dagger in docker compose -
1. Clone Dagger repository into your local

   ```shell
   git clone https://github.com/odpf/dagger.git
   ```
2. cd into the enrichment directory:
   ```shell
   cd dagger/quickstart/examples/enrichment 
   ```
3. fire this command to spin up the docker compose:
   ```shell
   docker compose up 
   ```
This will spin up docker containers for the kafka, zookeeper, stencil, kafka-producer and the dagger.
4. fire this command to gracefully close the docker compose:
   ```shell
   docker compose down 
   ```
   