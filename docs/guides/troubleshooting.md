# Troubleshooting

This section contains guides, best practices and advice related to troubleshooting issues/failures with Dagger in production.

`Note` : _If you are using dagger in production it is really important to set up the monitoring stack accordingly. Will be referring to a lot of dashboard panels here._

## Troubleshooting Issues

Listing some of the most frequent errors bellow and how to debug them.

### Error : `java.util.concurrent.TimeoutException: Heartbeat of TaskManager with id xxx timed out ...`[add image]

- This issue mostly means some issues with the checkpointing of states in Dagger though it can happen due to other reasons.
- You confirm this by checking if the checkpoint dashboards. The “Number of failed Checkpoints” dashboard would have an increasing curve and the last checkpoint size would be high (500+MB).

- If the checkpoint size is in MBs, you can scale up the job(increase parallelism to fix this). Try increasing the parallelism by 1 or 2 depending on the state size and increase it gradually. Don’t forget to reset the offset after scaling up the job.
- But if the checkpoint size near GBs, you may have to make some changes in the query like reducing window size to your existing query.

### Error : `org.influxdb.InfluxDBException: {"error":"unable to parse: invalid number...`

- This means you are pushing a time field (except rowtime) to influxDB without casting it to String/Varchar.
- You have to do something like CAST(TUMBLESTART(eventtimestamp, INTERVAL 1 MINUTE) AS VARCHAR).

### Error : `Caused by: java.lang.IllegalArgumentException: Expecting a positive number for fields size...`

- This happens in case of Daggers influx sink when you have selected all fields are tags and no fields are selected as fields.
- Influx does not let you push Data points that do not have a field value. So you need to select a field here.

### Error : `org.influxdb.InfluxDBException: tag limit exceeded...`

- Tags in Influx are indexed which are meant to make queries on tags faster. But there is a limit to storing the number of tags per measurement which is 200k in Daggers.
- So you should use a tag on a field that has a < 200k unique values.

### Error: `Flink Invalid topic Exception`

- Check if your output topic name has some special characters or white spaces. Flink does not let you create topics with these characters.

### Error: `Column 'xxx' not found in any table`

- Check the fields of the proto being used. Ensure the field is present and there are no typo for nested fields.
- However, if the field name is a keyword like ‘user’ you have to escape the keyword using a backtick `. Eg: x.`user`.y. For the list of keywords please refer [this](https://calcite.apache.org/docs/reference.html#keywords).

## FAQs

Here answering some of the most frequent questions related to dagger.
Please raise an issue in case you have more doubts.

### Why isn't my Dagger giving any results?

- Check if the Dagger is processing any data. You can check this from the “Records Rate - All stages” dashboard from the monitoring section.
- If there are any filters in the query, try to reduce the threshold or change any specific filter because of which it might not be outputting data. You can also try ensuring if you have written the filters with the correct data type.
- Check your window duration and watermark delay, the output data will be delayed by the window duration + watermark delay you have provided.

### How do I verify/print the data generated to Kafka?

- `Topic creation verification`: Use the following command to check if the topic created by dagger. Also if your auto topic creation is disabled please ensure you have created a topic beforehand for dagger to produce the data.

  ```
  ./bin/kafka-topics.sh --list --zookeeper localhost:2181
  ```

- `Data verification`: You can use any standard kafka consumer that support protobuf encoding in data to verify this.We have a custom consumer called [firehose](github.com/odpf/firehose). You can run a simple log sink firehose for this.

### Why can I not find the Topic created by Daggers?

- Make sure you have auto topic creation enabled on kafka.If not you have to manually create the output topic beforehand.
- Verify that the Dagger has started pushing data. Make sure if the Dagger has started producing Data to the output topic. Then only you will be able to import the topic.
- You can verify this in the “Records send Rate/Second” dashboard of the “Kafka producer” panel on the monitoring dashboard.Verify-in the health dashboard and error tab that the Dagger is not restarting.

### How to reset the offset of input Kafka topic for a given time?

- Have a kafka consumer start from that particular time. Follow [this](https://stackoverflow.com/questions/47391586/kafka-0-11-reset-offset-for-consumer-group-by-to-datetime) on how to do that.

### How is the time window in Dagger computed? When in the window results are produced?

- The time window will be aligned with the start of the epoch timestamp (1970 00:00:00). The start and end of the window will be computed based on that. The result will be materialized only at the end of the window.
- Let’s say I started my Dagger with a 39-minute interval. Based on that, the first window might only compute data less than 39 minutes. After that results will be produced every 39 minutes.

### Why is the Join condition not working for me?

- A common problem with using joins with streams is that for a given window there could be more than one message for the key, hence the join might not work properly
- One workaround for this is to use WITH, filter out or use distinct and create substreams and then perform the join, but this can have performance implications
- If you can do that in the main query, you can avoid using WITH
  Note that while using join, there should be only one windowing (tumble/hop) function

- For the watermark interval, keep it the same as the windowed time.
  - TUMBLE (rowtime, INTERVAL '10' MINUTE) use 10 Minute as the watermark interval.
  - HOP( iterations_log.rowtime, INTERVAL '1' MINUTE, INTERVAL '60' MINUTE ) use 60 Minute as the watermark interval.
- Watermark delay in case of windowed query should be equal to the duration of join. In case you join is not a windowed query, delay and interval will not impact you. Left join is supported, but for it to be effective, you need to ensure the data has unique keys in the respective stream before the join operation.
- Find sample queries for different types of joins [here](update link).

### Why is my Left Join query not producing any Data?

For Left Join to be effective, you need to ensure the data have unique keys in the respective stream before they join. Else the join doesn’t work.

### How to do aggregation on a nested value in Dagger?

Yes, you can access nested values with a ‘.’. So a field id inside a complex data field user can be accessed as something similar to **user.id**.

### I want to consume data from high throughput topics, what should I do?

For high throughput topics like dls and kong, ensure your parallelism is > 1. Also if you are okay with some delay in results, add a watermark delay of 30s or more. Once you're done, you can check if the Dagger can handle the load using this dashboard. There is a section for dropped data in the dashboard, this indicates the number of events not considered by Dagger for aggregation because they arrived late. Generally, the data is late because:

- The consumer (Dagger in this case) is lagging a lot. (Number varies depending on the throughput of the topic, please confirm before forming an opinion about what is the right number for you) - Increasing parallelism will help here.
- The events are coming late/out of order - Adding watermark delay will help here.
- You can also look into the producer throughput for a better understanding of the Data.

### Is my desired SQL function supported by Dagger?

- Check if the desired function is calcite supported. You can find all the calcite functions listed here. If the given function is present please have a look at the inbuilt function by Flink here . If the function is supported, you can directly use.
- If the function is not present in both places, check out the list of built-in functions developed by us here .
- Still can not find any suitable function to be used. Don’t worry it can be added [accordingly](update link).

### How Retry is handled in the post processors?

In case of errors from the external system of preprocessor Dagger, it will restart if the fail_on_error flag is set. With the restarting Dagger, it will keep on retrying from the point it restarted. So it's kind of a pseudo retry.
