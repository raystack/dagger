package org.raystack.dagger.integrationtest;

import org.raystack.dagger.common.core.DaggerContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.common.core.StreamInfo;
import org.raystack.dagger.core.processors.PostProcessorFactory;
import org.raystack.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import org.raystack.dagger.core.processors.types.PostProcessor;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.raystack.dagger.common.core.Constants.INPUT_STREAMS;
import static org.raystack.dagger.core.utils.Constants.PROCESSOR_POSTPROCESSOR_CONFIG_KEY;
import static org.raystack.dagger.core.utils.Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY;
import static io.vertx.pgclient.PgPool.pool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
@Ignore
public class PostGresExternalPostProcessorIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostGresExternalPostProcessorIntegrationTest.class.getName());
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());
    private static HashMap<String, String> configurationMap = new HashMap<>();
    private static PgPool pgClient;
    private static String host;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private final MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();

    private static DaggerContext daggerContext;

    private static StreamExecutionEnvironment streamExecutionEnvironment;

    private static StreamTableEnvironment streamTableEnvironment;

    private static Configuration configuration;


    @BeforeClass
    public static void setUp() {
        host = System.getenv("PG_HOST");
        if (StringUtils.isEmpty(host)) {
            host = "localhost";
        }

        String streams = "[{\"SOURCE_KAFKA_TOPIC_NAMES\":\"dummy-topic\",\"INPUT_SCHEMA_TABLE\":\"testbooking\",\"INPUT_SCHEMA_PROTO_CLASS\":\"org.raystack.dagger.consumer.TestBookingLogMessage\",\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\":\"41\",\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"localhost:6668\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"test-consumer\",\"SOURCE_KAFKA_NAME\":\"localkafka\"}]";
        configurationMap.put(PROCESSOR_POSTPROCESSOR_ENABLE_KEY, "true");
        configurationMap.put(INPUT_STREAMS, streams);
        configuration = new Configuration(ParameterTool.fromMap(configurationMap));

        daggerContext = mock(DaggerContext.class);
        streamExecutionEnvironment = mock(StreamExecutionEnvironment.class);
        streamTableEnvironment = mock(StreamTableEnvironment.class);
        setMock(daggerContext);
        when(daggerContext.getConfiguration()).thenReturn(configuration);
        when(daggerContext.getExecutionEnvironment()).thenReturn(streamExecutionEnvironment);
        when(daggerContext.getTableEnvironment()).thenReturn(streamTableEnvironment);

        try {

            pgClient = getPGClient();

            CountDownLatch countDownLatch = new CountDownLatch(1);

            pgClient.query("CREATE TABLE customer(customer_id integer PRIMARY KEY, surge_factor DOUBLE PRECISION UNIQUE NOT NULL);")
                    .execute(ar -> {
                        if (ar.succeeded()) {
                            LOGGER.info("table created successfully");
                            LOGGER.info("inserting data now");
                            pgClient.query("INSERT INTO customer values(123,23.33);")
                                    .execute(ar2 -> {
                                        if (ar2.succeeded()) {
                                            LOGGER.info("data inserted successfully");
                                            countDownLatch.countDown();
                                        }
                                    });
                        }
                    });

            countDownLatch.await(10L, TimeUnit.SECONDS);
            pgClient.close();
            LOGGER.info("Setup is completed");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void resetSingleton() throws Exception {
        Field instance = DaggerContext.class.getDeclaredField("daggerContext");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    private static void setMock(DaggerContext mock) {
        try {
            Field instance = DaggerContext.class.getDeclaredField("daggerContext");
            instance.setAccessible(true);
            instance.set(instance, mock);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static PgPool getPGClient() {

        String dbHost = System.getenv("PG_HOST");
        if (StringUtils.isEmpty(dbHost)) {
            dbHost = "localhost";
        }

        if (pgClient == null) {

            PgConnectOptions connectOptions = new PgConnectOptions()
                    .setPort(5432)
                    .setHost(dbHost)
                    .setDatabase("postgres")
                    .setUser("postgres")
                    .setPassword("postgres")
                    .setConnectTimeout(5000)
                    .setIdleTimeout(5000);

            PoolOptions poolOptions = new PoolOptions()
                    .setMaxSize(30);

            pgClient = pool(connectOptions, poolOptions);

        }
        return pgClient;
    }

    @Test
    public void shouldPopulateFieldFromPostgresWithCorrespondingDataType() throws Exception {

        String postProcessorConfigString =
                "{\n"
                        + "  \"external_source\": {\n"
                        + "    \"pg\": [\n"
                        + "      {\n"
                        + "        \"host\": \"" + host + "\",\n"
                        + "        \"port\": \"5432\",\n"
                        + "        \"user\": \"postgres\",\n"
                        + "        \"password\": \"postgres\",\n"
                        + "        \"database\": \"postgres\",\n"
                        + "        \"query_pattern\": \"select surge_factor from customer where customer_id = %s;\",\n"
                        + "        \"query_variables\": \"customer_id\",\n"
                        + "        \"stream_timeout\": \"5000\",\n"
                        + "        \"connect_timeout\": \"5000\",\n"
                        + "        \"idle_timeout\": \"5000\",\n"
                        + "        \"fail_on_errors\": \"false\", \n"
                        + "        \"capacity\": \"30\",\n"
                        + "        \"type\": \"org.raystack.dagger.consumer.TestSurgeFactorLogMessage\", \n"
                        + "        \"output_mapping\": {\n"
                        + "          \"surge_factor\": \"surge_factor\"   \n"
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

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "dummy-order-id");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals(23.33F, (Float) CollectSink.OUTPUT_VALUES.get(0).getField(0), 0.0F);
    }

    @Test
    public void shouldPopulateFieldFromPostgresWithSuccessResponseWithExternalAndInternalSource() throws Exception {
        String postProcessorConfigWithInternalSourceString =
                "{\n"
                        + "  \"external_source\": {\n"
                        + "    \"pg\": [\n"
                        + "      {\n"
                        + "        \"host\": \"" + host + "\",\n"
                        + "        \"port\": \"5432\",\n"
                        + "        \"user\": \"postgres\",\n"
                        + "        \"password\": \"postgres\",\n"
                        + "        \"database\": \"postgres\",\n"
                        + "        \"query_pattern\": \"select surge_factor from customer where customer_id = %s;\",\n"
                        + "        \"query_variables\": \"customer_id\",\n"
                        + "        \"stream_timeout\": \"5000\",\n"
                        + "        \"connect_timeout\": \"5000\",\n"
                        + "        \"idle_timeout\": \"5000\",\n"
                        + "        \"fail_on_errors\": \"false\", \n"
                        + "        \"capacity\": \"30\",\n"
                        + "        \"type\": \"org.raystack.dagger.consumer.TestSurgeFactorLogMessage\", \n"
                        + "        \"output_mapping\": {\n"
                        + "          \"surge_factor\": \"surge_factor\"   \n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  },\n"
                        + "    \"internal_source\": [\n"
                        + "       {"
                        + "       \"output_field\": \"event_timestamp\", \n"
                        + "       \"type\": \"function\",\n"
                        + "       \"value\": \"CURRENT_TIMESTAMP\"\n"
                        + "       }"
                        + "   ]"
                        + "}";

        configurationMap.put(PROCESSOR_POSTPROCESSOR_CONFIG_KEY, postProcessorConfigWithInternalSourceString);
        configuration = new Configuration(ParameterTool.fromMap(configurationMap));
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "dummy-order-id");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();

        assertTrue(CollectSink.OUTPUT_VALUES.get(0).getField(1) instanceof Timestamp);
        assertEquals(23.33F, (Float) CollectSink.OUTPUT_VALUES.get(0).getField(0), 0.0F);
    }

    @Test
    public void shouldPopulateFieldFromPostgresOnSuccessResponseWithAllThreeSourcesIncludingTransformer() throws Exception {

        String postProcessorConfigWithTransformerString =
                "{\n"
                        + "  \"external_source\": {\n"
                        + "    \"pg\": [\n"
                        + "      {\n"
                        + "        \"host\": \"" + host + "\",\n"
                        + "        \"port\": \"5432\",\n"
                        + "        \"user\": \"postgres\",\n"
                        + "        \"password\": \"postgres\",\n"
                        + "        \"database\": \"postgres\",\n"
                        + "        \"query_pattern\": \"select surge_factor from customer where customer_id = %s;\",\n"
                        + "        \"query_variables\": \"customer_id\",\n"
                        + "        \"stream_timeout\": \"5000\",\n"
                        + "        \"connect_timeout\": \"5000\",\n"
                        + "        \"idle_timeout\": \"5000\",\n"
                        + "        \"fail_on_errors\": \"false\", \n"
                        + "        \"capacity\": \"30\",\n"
                        + "        \"type\": \"org.raystack.dagger.consumer.TestSurgeFactorLogMessage\", \n"
                        + "        \"output_mapping\": {\n"
                        + "          \"surge_factor\": \"surge_factor\"   \n"
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
                        + "       \"output_field\": \"customer_id\", \n"
                        + "       \"type\": \"sql\",\n"
                        + "       \"value\": \"customer_id\"\n"
                        + "       }"
                        + "   ]"
                        + ",\n"
                        + " \"transformers\": ["
                        + "{\n"
                        + "  \"transformation_class\": \"org.raystack.dagger.functions.transformers.ClearColumnTransformer\",\n"
                        + "  \"transformation_arguments\": {\n"
                        + "    \"targetColumnName\": \"customer_id\"\n"
                        + "  }\n"
                        + "}\n"
                        + "   ]   \n"
                        + "}";

        configurationMap.put(PROCESSOR_POSTPROCESSOR_CONFIG_KEY, postProcessorConfigWithTransformerString);
        configuration = new Configuration(ParameterTool.fromMap(configurationMap));
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "dummy-order-id");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());


        env.execute();
        assertEquals(23.33F, (Float) CollectSink.OUTPUT_VALUES.get(0).getField(0), 0.0F);
        assertEquals("", CollectSink.OUTPUT_VALUES.get(0).getField(2));

        assertTrue(CollectSink.OUTPUT_VALUES.get(0).getField(1) instanceof Timestamp);
    }


    private StreamInfo addPostProcessor(StreamInfo streamInfo) {
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(daggerContext, stencilClientOrchestrator, streamInfo.getColumnNames(), telemetryExporter);
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
