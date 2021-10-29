package io.odpf.dagger.integrationtest;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.PostProcessorFactory;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.types.PostProcessor;
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
import java.util.HashMap;
import java.util.List;

import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static io.odpf.dagger.core.utils.Constants.PROCESSOR_POSTPROCESSOR_CONFIG_KEY;
import static io.odpf.dagger.core.utils.Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class EsExternalPostProcessorIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(EsExternalPostProcessorIntegrationTest.class.getName());

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());
    private StencilClientOrchestrator stencilClientOrchestrator;
    private final MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();
    private RestClient esClient;
    private String host;
    private Configuration configuration;
    private HashMap<String, String> configurationMap;

    @Before
    public void setUp() {
        String streams = "[{\"SOURCE_KAFKA_TOPIC_NAMES\":\"dummy-topic\",\"INPUT_SCHEMA_TABLE\":\"testbooking\",\"INPUT_SCHEMA_PROTO_CLASS\":\"io.odpf.dagger.consumer.TestBookingLogMessage\",\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\":\"41\",\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"localhost:6668\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"test-consumer\",\"SOURCE_KAFKA_NAME\":\"localkafka\"}]";
        this.configurationMap = new HashMap<>();
        configurationMap.put(PROCESSOR_POSTPROCESSOR_ENABLE_KEY, "true");
        configurationMap.put(INPUT_STREAMS, streams);
        this.configuration = new Configuration(ParameterTool.fromMap(configurationMap));
        host = System.getenv("ES_HOST");
        if (StringUtils.isEmpty(host)) {
            host = "localhost";
        }
        esClient = getESClient();
        Request insertRequest = new Request("POST", "/customers/_doc/123");
        insertRequest.setJsonEntity(
                "{\n"
                        + "  \"customer_id\": \"123\",\n"
                        + "  \"event_timestamp\": {\n"
                        + "    \"seconds\": 1590726932\n"
                        + "  },\n"
                        + "  \"name\": \"dummy user\",\n"
                        + "  \"email\": \"dummyuser123@gmail.com\",\n"
                        + "  \"phone\": \"+22222\",\n"
                        + "  \"phone_verified\": true,\n"
                        + "  \"active\": true,\n"
                        + "  \"wallet_id\": \"11111\",\n"
                        + "  \"signed_up_country\": \"ID\",\n"
                        + "  \"locale\": \"en\",\n"
                        + "  \"created_at\": {\n"
                        + "    \"seconds\": 1498540716\n"
                        + "  },\n"
                        + "  \"updated_at\": {\n"
                        + "    \"seconds\": 1556087608\n"
                        + "  }\n"
                        + "}");

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
        String postProcessorConfigString =
                "{\n"
                        + "  \"external_source\": {\n"
                        + "    \"es\": [\n"
                        + "      {\n"
                        + "        \"host\":" + host + ",\n"
                        + "        \"port\": \"49153\",\n"
                        + "        \"endpoint_pattern\": \"/customers/_doc/%s\",\n"
                        + "        \"endpoint_variables\": \"customer_id\",\n"
                        + "        \"stream_timeout\": \"5000\",\n"
                        + "        \"connect_timeout\": \"5000\",\n"
                        + "        \"socket_timeout\": \"5000\",\n"
                        + "        \"retry_timeout\": \"5000\",\n"
                        + "        \"capacity\": \"30\",\n"
                        + "        \"type\": \"io.odpf.dagger.consumer.TestEnrichedBookingLogMessage\", \n"
                        + "        \"output_mapping\": {\n"
                        + "          \"customer_profile\": {\n"
                        + "            \"path\": \"$._source\"\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }\n"
                        + "}";

        configurationMap.put(PROCESSOR_POSTPROCESSOR_CONFIG_KEY, postProcessorConfigString);
        configuration = new Configuration(ParameterTool.fromMap(configurationMap));
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"order_number", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "dummy-customer-id");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals("123", CollectSink.OUTPUT_VALUES.get(0).getField(0).toString().split(",")[0]);
        assertEquals("dummyuser123@gmail.com", CollectSink.OUTPUT_VALUES.get(0).getField(0).toString().split(",")[2]);
    }

    @Test
    public void shouldPopulateFieldFromESOnSuccessResponseWithExternalAndInternalSource() throws Exception {
        String postProcessorConfigString =
                "{\n"
                        + "  \"external_source\": {\n"
                        + "    \"es\": [\n"
                        + "      {\n"
                        + "        \"host\":" + host + ",\n"
                        + "        \"port\": \"49153\",\n"
                        + "        \"endpoint_pattern\": \"/customers/_doc/%s\",\n"
                        + "        \"endpoint_variables\": \"customer_id\",\n"
                        + "        \"stream_timeout\": \"5000\",\n"
                        + "        \"connect_timeout\": \"5000\",\n"
                        + "        \"socket_timeout\": \"5000\",\n"
                        + "        \"retry_timeout\": \"5000\",\n"
                        + "        \"capacity\": \"30\",\n"
                        + "        \"type\": \"io.odpf.dagger.consumer.TestEnrichedBookingLogMessage\", \n"
                        + "        \"output_mapping\": {\n"
                        + "          \"customer_profile\": {\n"
                        + "            \"path\": \"$._source\"\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  },\n"
                        + "    \"internal_source\": [\n"
                        + "       {"
                        + "       \"output_field\": \"event_timestamp\", \n"
                        + "       \"type\": \"function\",\n"
                        + "       \"value\": \"CURRENT_TIMESTAMP\"\n"
                        + "       },"
                        + "       {"
                        + "       \"output_field\": \"driver_id\", \n"
                        + "       \"type\": \"sql\",\n"
                        + "       \"value\": \"driver_id\"\n"
                        + "       }"
                        + "   ]"
                        + "}";

        configurationMap.put(PROCESSOR_POSTPROCESSOR_CONFIG_KEY, postProcessorConfigString);
        configuration = new Configuration(ParameterTool.fromMap(configurationMap));

        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"order_number", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "dummy-customer-id");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals("123", CollectSink.OUTPUT_VALUES.get(0).getField(0).toString().split(",")[0]);
        assertEquals("dummy user", CollectSink.OUTPUT_VALUES.get(0).getField(0).toString().split(",")[1]);
        assertTrue(CollectSink.OUTPUT_VALUES.get(0).getField(1) instanceof Timestamp);
        assertEquals("456", CollectSink.OUTPUT_VALUES.get(0).getField(2));

    }

    @Test
    public void shouldPopulateFieldFromESOnSuccessResponseWithAllThreeSourcesIncludingTransformer() throws Exception {
        String postProcessorConfigString =
                "{\n"
                        + "  \"external_source\": {\n"
                        + "    \"es\": [\n"
                        + "      {\n"
                        + "        \"host\":" + host + ",\n"
                        + "        \"port\": \"49153\",\n"
                        + "        \"endpoint_pattern\": \"/customers/_doc/%s\",\n"
                        + "        \"endpoint_variables\": \"customer_id\",\n"
                        + "        \"stream_timeout\": \"5000\",\n"
                        + "        \"connect_timeout\": \"5000\",\n"
                        + "        \"socket_timeout\": \"5000\",\n"
                        + "        \"retry_timeout\": \"5000\",\n"
                        + "        \"capacity\": \"30\",\n"
                        + "        \"type\": \"io.odpf.dagger.consumer.TestEnrichedBookingLogMessage\", \n"
                        + "        \"output_mapping\": {\n"
                        + "          \"customer_profile\": {\n"
                        + "            \"path\": \"$._source\"\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  },\n"
                        + "    \"internal_source\": [\n"
                        + "       {"
                        + "       \"output_field\": \"event_timestamp\", \n"
                        + "       \"type\": \"function\",\n"
                        + "       \"value\": \"CURRENT_TIMESTAMP\"\n"
                        + "       },"
                        + "       {"
                        + "       \"output_field\": \"driver_id\", \n"
                        + "       \"type\": \"sql\",\n"
                        + "       \"value\": \"driver_id\"\n"
                        + "       }"
                        + "   ]"
                        + ",\n"
                        + " \"transformers\": ["
                        + "{\n"
                        + "  \"transformation_class\": \"io.odpf.dagger.functions.transformers.ClearColumnTransformer\",\n"
                        + "  \"transformation_arguments\": {\n"
                        + "    \"targetColumnName\": \"driver_id\"\n"
                        + "  }\n"
                        + "}\n"
                        + "   ]   \n"
                        + "}";

        configurationMap.put(PROCESSOR_POSTPROCESSOR_CONFIG_KEY, postProcessorConfigString);
        configuration = new Configuration(ParameterTool.fromMap(configurationMap));

        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"order_number", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "dummy-customer-id");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals("123", CollectSink.OUTPUT_VALUES.get(0).getField(0).toString().split(",")[0]);
        assertEquals("true", CollectSink.OUTPUT_VALUES.get(0).getField(0).toString().split(",")[3]);
        assertTrue(CollectSink.OUTPUT_VALUES.get(0).getField(1) instanceof Timestamp);
        assertEquals("", CollectSink.OUTPUT_VALUES.get(0).getField(2));
    }


    private RestClient getESClient() {
        HttpHost localhost = new HttpHost(host, 49153);
        return RestClient.builder(localhost)
                .setRequestConfigCallback(requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(5000)
                                .setSocketTimeout(5000))
                .setMaxRetryTimeoutMillis(5000)
                .build();
    }

    private StreamInfo addPostProcessor(StreamInfo streamInfo) {
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, streamInfo.getColumnNames(), telemetryExporter);
        StreamInfo postProcessedStream = streamInfo;
        for (PostProcessor postProcessor : postProcessors) {
            postProcessedStream = postProcessor.process(postProcessedStream);
        }
        return postProcessedStream;
    }

    private static class CollectSink implements SinkFunction<Row> {

        static final List<Row> OUTPUT_VALUES = new ArrayList<>();

        @Override
        public synchronized void invoke(Row inputRow, Context context) {
            OUTPUT_VALUES.add(inputRow);
        }
    }
}
