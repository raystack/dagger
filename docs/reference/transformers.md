# Transformers

This page contains reference for all the custom transformers available on Dagger.

## List of Transformers

- [ClearColumnTransformer](transformers.md#ClearColumnTransformer)
- [DeDuplicationTransformer](transformers.md#DeDuplicationTransformer)
- [FeatureTransformer](transformers.md#FeatureTransformer)
- [FeatureWithTypeTransformer](transformers.md#FeatureWithTypeTransformer)
- [HashTransformer](transformers.md#HashTransformer)
- [InvalidRecordFilterTransformer](transformers.md#InvalidRecordFilterTransformer)
- [SQLTransformer](transformers.md#SQLTransformer)

### ClearColumnTransformer
* Transformation Class:
  * `io.odpf.dagger.functions.transformers.ClearColumnTransformer`
* Contract: 
  * Mandatory post-processing here. After Selecting columns by SQL, you have to reselect by an internal source. Following transformation arguments can be passed:
    * `targetColumnName`: The field that need to be cleared.
* Functionality:
  * Allows to clear the specified column of data produced by the dagger.
* Example:
  * SQL:
    ```
    SELECT
      event_timestamp,
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
          "output_field": "*",
          "value": "*",
          "type": "sql"
        }
      ],
      "transformers": [
        {
          "transformation_class": "io.odpf.dagger.functions.transformers.ClearColumnTransformer",
          "transformation_arguments": {
            "targetColumnName": "data1"
          }
        }
      ]
    }
    ```

### DeDuplicationTransformer
* Transformation Class:
  * `io.odpf.dagger.functions.transformers.DeDuplicationTransformer`
* Contract: 
  * Mandatory post-processing here. After Selecting columns by SQL, you have to reselect by an internal source. Following transformation arguments can be passed:
    * `key_column`: This value will be used as the deduplication key (other events with the same key will be stopped). 
    * `ttl_in_seconds`: The TTL configuration which will decide how long to keep the keys in memory. Once the keys are cleared from memory the data with the same keys will be sent again.
* Functionality:
  * Allows to deduplicate data produced by the dagger i.e records with the same key will not be sent again till the TTL expires.
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
          "transformation_class": "com.gojek.dagger.transformer.DeDuplicationTransformer"
        }
      ]
    }
    ```

### FeatureTransformer
* Transformation Class:
  * `io.odpf.dagger.functions.transformers.FeatureTransformer`
* Contract: 
  * Mandatory post-processing here. After Selecting columns by SQL, you have to reselect by an internal source. Following transformation arguments can be passed:
    * `keyColumnName`: This value will be used to form the key of the feature. 
    * `valueColumnName`: This value will be used as a value in the feature.
* Functionality:
  * Converts to feast Features from post processors.
* Example:
  * SQL:
    ```
    // You can select the fields that want to retain as part of enrichment or you want to use for making the request.
    SELECT 
      data1, 
      features 
    FROM
      data_stream
    ```
  * POST PROCESSOR CONFIG:
    ```
    // This configuration is used with other external postprocessors like `http`. Using the below example, the output of http will be transformed to featureRow using the transformation.
    {
      "internal_source": [
        {
          "output_field": "*",
          "value": "*",
          "type": "sql"
        }
      ],
      "transformers": [
    	{
      	"transformation_arguments": {
        	"keyColumnName": "data1",
        	"valueColumnName": "features"
      	},
      	"transformation_class": "com.gojek.dagger.transformer.FeatureTransformer"
    	}
      ]
    }
    ```

### FeatureWithTypeTransformer
* Transformation Class:
  * `io.odpf.dagger.functions.transformers.FeatureWithTypeTransformer`
* Contract: 
  * Mandatory post-processing here. After Selecting columns by SQL, you have to reselect by an internal source. Following transformation arguments can be passed:
    * `outputColumnName`: The column where the final feature will be written and data are synonyms with FeatureWithType UDF and a single feature is represented by an element in an array.
* Functionality:
  * Converts to feast Features from post processors. This is required to do aggregation and feature transformation from a single dagger.
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
          "output_field": "features",
          "value": "test",
          "type": "constant"
        },
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
          "transformation_class": "com.gojek.dagger.transformer.FeatureWithTypeTransformer",
          "transformation_arguments": {
            "outputColumnName": "features",
            "data": [
              {
                "keyColumnName": "data1",
                "valueColumnName": "data2",
                "type": "StringType"
              }
            ]
          }
        }
      ]
    }
    ```

### HashTransformer
* Transformation Class:
  * `io.odpf.dagger.functions.transformers.HashTransformer`
* Contract: 
  * Mandatory post-processing here. After Selecting columns by SQL, you have to reselect by an internal source. Following transformation arguments can be passed:
    * `maskColumns`(mandatory): The fields that need to be encrypted/masked.
* Functionality:
  * Enables encryption on a set of fields as configured. Used in Data forwarding daggers to clone production data to integration environments with encryption on sensitive data fields. We are using SHA-256 hashing to encrypt data.
* Limitations:
  * Currently support masking on Non Complex Fields of Data type Integer, Big Integer and String. However you can encrypt nested fields of complex data using `.` notations. For example test_data.customer_id is a valid argument which will encrypt the customer_id inside test_data. 
  * All other Data types including Arrays, complex fields and other primitive types like boolean are not supported.
* Example:
  * SQL:
    ```
    SELECT
      event_timestamp,
      test_data
    FROM
      data_stream
    ```
  * POST PROCESSOR CONFIG:
    ```
    {
      "internal_source": [
        {
          "output_field": "*",
          "value": "*",
          "type": "sql"
        }
      ],
      "transformers": [
        {
          "transformation_class": "com.gojek.dagger.transformer.HashTransformer",
          "transformation_arguments": {
            "maskColumns": [
              "test_data.data1"
            ]
          }
        }
      ]
    }
    ```

### InvalidRecordFilterTransformer
* Transformation Class:
  * `io.odpf.dagger.functions.transformers.InvalidRecordFilterTransformer`
* Contract: 
  * Following transformation config can be passed:
    * `table_transformers`: A list of transformer configs.
    * `table_name`: Table name for the transformer.
    * `transformers`: List of transformers per table.
    * `transformation_class`: Fully qualified name of the class to be used for transformation.
    * `transformation_arguments`: A key-value map required for parameters required for the custom transformation class.
* Functionality:
  * Filter the invalid records produced by dagger.
* Example:
  * SQL:
    ```
    SELECT 
      data1, 
      data2, 
      event_timestamp 
    FROM 
      data_stream
    ```
  * PRE PROCESSOR CONFIG:
    ```
    {
      "table_transformers": [
        {
          "table_name": "testtable",
          "transformers": [
            {
              "transformation_class": "io.odpf.dagger.functions.transformers.InvalidRecordFilterTransformer"
            }
          ]
        }
      ]
    }
    ```

### SQLTransformer
* Transformation Class:
  * `io.odpf.dagger.functions.transformers.SQLTransformer`
* Contract: 
  * Mandatory post-processing here. After Selecting columns by SQL, you have to reselect by an internal source. Following transformation arguments can be passed:
      * `sqlQuery`(mandatory): The SQL query for transformation
      * `tableName`(optional): The table name to be used in the above SQL(default: data_stream)
      * `allowedLatenessInMs`(optional): The allowed lateness for the events streaming in Kafka(default: 0))
* Functionality:
  * Enables to apply a SQL transformation on top of streaming data in post processors. Primarily useful if users want to apply SQL transformation/aggregation using fields added via External/Internal Post Processors.
* Example:
  * SQL:
    ```
    SELECT
      data1,
      data2,
      rowtime
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
          "output_field": "rowtime",
          "value": "rowtime",
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
          "transformation_class": "com.gojek.dagger.transformer.SQLTransformer",
          "transformation_arguments": {
            "sqlQuery": "SELECT count(distinct data1) AS `count`, data2, TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS event_timestamp FROM data_stream group by TUMBLE (rowtime, INTERVAL '60' SECOND), data2"
          }
        }
      ]
    }
    ```
  

