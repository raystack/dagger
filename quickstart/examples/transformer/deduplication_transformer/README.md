### DeDuplicationTransformer
* Transformation Class:
    * `io.odpf.dagger.functions.transformers.DeDuplicationTransformer`
* Contract:
    * After Selecting columns by SQL, you need to reselect the desired columns with the help of an internal source. Following transformation arguments can be passed:
        * `key_column`: This value will be used as the deduplication key (other events with the same key will be stopped).
        * `ttl_in_seconds`: The TTL configuration will decide how long to keep the keys in memory. Once the keys are cleared from memory the data with the same keys will be sent again.
* Functionality:
    * Allows deduplication of data produced by the dagger i.e records with the same key will not be sent again till the TTL expires.
    * Can be used both on `post-processor` and `pre-processor`
* Example:
    * SQL:
      ```
      SELECT
        data1,
        data2
      FROM
        data_stream
      ```
    * POST PROCESSOR CONFIG:
      ```
      {
        "internal_source": [
          {
            "output_field": "data1",
            "value": "data1",
            "type": "sql"
          },
          {
            "output_field": "data2",
            "value": "data2",
            "type": "sql"
          }
        ],
        "transformers": [
          {
            "transformation_arguments": {
              "key_column": "data1",
              "ttl_in_seconds": "3600"
            },
            "transformation_class": "io.odpf.dagger.functions.transformers.DeDuplicationTransformer"
          }
        ]
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
2. cd into the transformer directory:
   ```shell
   cd dagger/quickstart/examples/transformer 
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
   