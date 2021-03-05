import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.postprocessors.PostProcessorFactory;
import com.gojek.daggers.postprocessors.common.PostProcessor;
import com.gojek.daggers.postprocessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.utils.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.gojek.daggers.utils.Constants.INPUT_STREAMS;
import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EsExternalPostProcessorIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(EsExternalPostProcessorIntegrationTest.class.getName());
    private StencilClientOrchestrator stencilClientOrchestrator;
    private MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();
    private Configuration configuration = new Configuration();
    private RestClient esClient;
    private String host;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());


    @Before
    public void setUp() {
        String streams = "[{\"TOPIC_NAMES\":\"SG_GO_CAR-booking-log\",\"TABLE_NAME\":\"booking\",\"PROTO_CLASS_NAME\":\"com.gojek.esb.booking.BookingLogMessage\",\"EVENT_TIMESTAMP_FIELD_INDEX\":\"41\",\"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"10.200.216.49:6668,10.200.219.198:6668,10.200.216.58:6668,10.200.216.54:6668,10.200.216.56:6668,10.200.216.63:6668\",\"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"\",\"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"test-config\",\"STREAM_NAME\":\"p-godata-id-mainstream\"}]";
        configuration.setString(POST_PROCESSOR_ENABLED_KEY, "true");
        configuration.setString(INPUT_STREAMS, streams);
        host = System.getenv("ES_HOST");
        if (StringUtils.isEmpty(host)) {
            host = "localhost";
        }
        esClient = getESClient();
        Request insertRequest = new Request("POST", "/customers/_doc/123");
        insertRequest.setJsonEntity("{\n" +
                "  \"customer_id\": \"123\",\n" +
                "  \"event_timestamp\": {\n" +
                "    \"seconds\": 1590726932\n" +
                "  },\n" +
                "  \"name\": \"dummy user\",\n" +
                "  \"email\": \"dummyuser123@gmail.com\",\n" +
                "  \"phone\": \"+022122232224\",\n" +
                "  \"phone_verified\": true,\n" +
                "  \"active\": true,\n" +
                "  \"wallet_id\": \"171780318401271436\",\n" +
                "  \"signed_up_country\": \"ID\",\n" +
                "  \"locale\": \"en\",\n" +
                "  \"created_at\": {\n" +
                "    \"seconds\": 1498540716\n" +
                "  },\n" +
                "  \"updated_at\": {\n" +
                "    \"seconds\": 1556087608\n" +
                "  }\n" +
                "}");

        try {
            Response response = esClient.performRequest(insertRequest);
            LOGGER.info("Insert data response: " + response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            LOGGER.warn("Could not post to ES", e);
        }

    }

    @After
    public void tearDown() {
        Request deleteRequest = new Request("DELETE", "/customers/");
        try {
            Response response = esClient.performRequest(deleteRequest);
            LOGGER.info("Delete data response: " + response.getStatusLine().getStatusCode());
            esClient.close();
        } catch (IOException e) {
            LOGGER.warn("Could not post to ES", e);
        }
    }

    @Test
    public void shouldPopulateFieldFromESOnSuccessResponse() throws Exception {
        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"es\": [\n" +
                "      {\n" +
                "        \"host\":" + host + ",\n" +
                "        \"port\": \"9200\",\n" +
                "        \"endpoint_pattern\": \"/customers/_doc/%s\",\n" +
                "        \"endpoint_variables\": \"customer_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"socket_timeout\": \"5000\",\n" +
                "        \"retry_timeout\": \"5000\",\n" +
                "        \"capacity\": \"30\",\n" +
                "        \"type\": \"com.gojek.esb.fraud.EnrichedBookingLogMessage\", \n" +
                "        \"output_mapping\": {\n" +
                "          \"customer_profile\": {\n" +
                "            \"path\": \"$._source\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EsExternalPostProcessorIntegrationTest.CollectSink.values.clear();

        String[] inputColumnNames = new String[]{"order_number", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals("123", CollectSink.values.get(0).getField(0).toString().split(",")[0]);
        assertEquals("dummyuser123@gmail.com", CollectSink.values.get(0).getField(0).toString().split(",")[4]);
    }

    @Test
    public void shouldPopulateFieldFromESOnSuccessResponseWithExternalAndInternalSource() throws Exception {
        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"es\": [\n" +
                "      {\n" +
                "        \"host\":" + host + ",\n" +
                "        \"port\": \"9200\",\n" +
                "        \"endpoint_pattern\": \"/customers/_doc/%s\",\n" +
                "        \"endpoint_variables\": \"customer_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"socket_timeout\": \"5000\",\n" +
                "        \"retry_timeout\": \"5000\",\n" +
                "        \"capacity\": \"30\",\n" +
                "        \"type\": \"com.gojek.esb.fraud.EnrichedBookingLogMessage\", \n" +
                "        \"output_mapping\": {\n" +
                "          \"customer_profile\": {\n" +
                "            \"path\": \"$._source\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "    \"internal_source\": [\n" +
                "       {" +
                "       \"output_field\": \"event_timestamp\", \n" +
                "       \"type\": \"function\",\n" +
                "       \"value\": \"CURRENT_TIMESTAMP\"\n" +
                "       }," +
                "       {" +
                "       \"output_field\": \"driver_id\", \n" +
                "       \"type\": \"sql\",\n" +
                "       \"value\": \"driver_id\"\n" +
                "       }" +
                "   ]" +
                "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EsExternalPostProcessorIntegrationTest.CollectSink.values.clear();

        String[] inputColumnNames = new String[]{"order_number", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals("123", CollectSink.values.get(0).getField(0).toString().split(",")[0]);
        assertEquals("dummy user", CollectSink.values.get(0).getField(0).toString().split(",")[3]);
        assertEquals("ID", CollectSink.values.get(0).getField(0).toString().split(",")[15]);
        assertTrue(CollectSink.values.get(0).getField(1) instanceof Timestamp);
        assertEquals("456", CollectSink.values.get(0).getField(2));

    }

    @Test
    public void shouldPopulateFieldFromESOnSuccessResponseWithAllThreeSourcesIncludingTransformer() throws Exception {
        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"es\": [\n" +
                "      {\n" +
                "        \"host\":" + host + ",\n" +
                "        \"port\": \"9200\",\n" +
                "        \"endpoint_pattern\": \"/customers/_doc/%s\",\n" +
                "        \"endpoint_variables\": \"customer_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"socket_timeout\": \"5000\",\n" +
                "        \"retry_timeout\": \"5000\",\n" +
                "        \"capacity\": \"30\",\n" +
                "        \"type\": \"com.gojek.esb.fraud.EnrichedBookingLogMessage\", \n" +
                "        \"output_mapping\": {\n" +
                "          \"customer_profile\": {\n" +
                "            \"path\": \"$._source\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "    \"internal_source\": [\n" +
                "       {" +
                "       \"output_field\": \"event_timestamp\", \n" +
                "       \"type\": \"function\",\n" +
                "       \"value\": \"CURRENT_TIMESTAMP\"\n" +
                "       }," +
                "       {" +
                "       \"output_field\": \"driver_id\", \n" +
                "       \"type\": \"sql\",\n" +
                "       \"value\": \"driver_id\"\n" +
                "       }" +
                "   ]" +
                ",\n" +
                " \"transformers\": [" +
                "{\n" +
                "  \"transformation_class\": \"com.gojek.dagger.transformer.ClearColumnTransformer\",\n" +
                "  \"transformation_arguments\": {\n" +
                "    \"targetColumnName\": \"driver_id\"\n" +
                "  }\n" +
                "}\n" +
                "   ]   \n" +
                "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EsExternalPostProcessorIntegrationTest.CollectSink.values.clear();

        String[] inputColumnNames = new String[]{"order_number", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals("123", CollectSink.values.get(0).getField(0).toString().split(",")[0]);
        assertEquals("+022122232224", CollectSink.values.get(0).getField(0).toString().split(",")[5]);
        assertEquals("true", CollectSink.values.get(0).getField(0).toString().split(",")[6]);
        assertTrue(CollectSink.values.get(0).getField(1) instanceof Timestamp);
        assertEquals("", CollectSink.values.get(0).getField(2));
    }


    private RestClient getESClient() {
        HttpHost localhost = new HttpHost(host, 9200);
        return RestClient.builder(localhost)
                .setRequestConfigCallback(requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(5000)
                                .setSocketTimeout(5000))
                .setMaxRetryTimeoutMillis(5000)
                .build();
    }

    private static class CollectSink implements SinkFunction<Row> {

        static final List<Row> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Row inputRow, Context context) {
            values.add(inputRow);
        }
    }

    private StreamInfo addPostProcessor(StreamInfo streamInfo) {
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, streamInfo.getColumnNames(), telemetryExporter);
        StreamInfo postProcessedStream = streamInfo;
        for (PostProcessor postProcessor : postProcessors) {
            postProcessedStream = postProcessor.process(postProcessedStream);
        }
        return postProcessedStream;
    }
}
