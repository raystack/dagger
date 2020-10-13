import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.PostProcessorFactory;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.utils.Constants;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.utils.Constants.INPUT_STREAMS;
import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_ENABLED_KEY;
import static io.vertx.pgclient.PgPool.pool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostGresExternalPostProcessorIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostGresExternalPostProcessorIntegrationTest.class.getName());
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());
    private StencilClientOrchestrator stencilClientOrchestrator;
    private MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();
    private static Configuration configuration = new Configuration();
    private static PgPool pgClient;
    private static String host;

    @BeforeClass
    public static void setUp() {
        host = System.getenv("PG_HOST");
        if (StringUtils.isEmpty(host)) {
            host = "localhost";
        }

        try {

            String streams = "[{\"TOPIC_NAMES\":\"SG_GO_CAR-booking-log\",\"TABLE_NAME\":\"booking\",\"PROTO_CLASS_NAME\":\"com.gojek.esb.booking.BookingLogMessage\",\"EVENT_TIMESTAMP_FIELD_INDEX\":\"41\",\"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"10.200.216.49:6668,10.200.219.198:6668,10.200.216.58:6668,10.200.216.54:6668,10.200.216.56:6668,10.200.216.63:6668\",\"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"\",\"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"test-config\",\"STREAM_NAME\":\"p-godata-id-mainstream\"}]";

            configuration.setString(POST_PROCESSOR_ENABLED_KEY, "true");

            configuration.setString(INPUT_STREAMS, streams);

            pgClient = getPGClient();

            CountDownLatch countDownLatch = new CountDownLatch(1);

            pgClient.query("CREATE TABLE customer(customer_id integer PRIMARY KEY, surge_factor DOUBLE PRECISION UNIQUE NOT NULL);")
                    .execute(ar -> {
                        if(ar.succeeded()) {
                            LOGGER.info("table created successfully");
                            LOGGER.info("inserting data now");
                            pgClient.query("INSERT INTO customer values(123,23.33);")
                                    .execute(ar2 -> {
                                        if(ar2.succeeded()) {
                                            LOGGER.info("data inserted successfully");
                                            countDownLatch.countDown();
                                        }
                                    });
                        }
                    });

            countDownLatch.await(10L, TimeUnit.SECONDS);
            pgClient.close();
            LOGGER.info("Setup is completed");


        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void shouldPopulateFieldFromPostgresWithCorrespondingDataType() throws Exception {

        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"pg\": [\n" +
                "      {\n" +
                "        \"host\": \"" + host + "\",\n" +
                "        \"port\": \"5432\",\n" +
                "        \"user\": \"root\",\n" +
                "        \"password\": \"root\",\n" +
                "        \"database\": \"test_db\",\n" +
                "        \"query_pattern\": \"select surge_factor from customer where customer_id = %s;\",\n" +
                "        \"query_variables\": \"customer_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"idle_timeout\": \"5000\",\n" +
                "        \"fail_on_errors\": \"false\", \n" +
                "        \"capacity\": \"30\",\n" +
                "        \"type\": \"com.gojek.esb.aggregate.surge.SurgeFactorLogMessage\", \n" +
                "        \"output_mapping\": {\n" +
                "          \"surge_factor\": \"surge_factor\"   \n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.values.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals(23.33F, (Float) CollectSink.values.get(0).getField(0), 0.0F);
    }

    @Test
    public void shouldPopulateFieldFromPostgresWithSuccessResponseWithExternalAndInternalSource() throws Exception {
        String postProcessorConfigWithInternalSourceString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"pg\": [\n" +
                "      {\n" +
                "        \"host\": \"" + host + "\",\n" +
                "        \"port\": \"5432\",\n" +
                "        \"user\": \"root\",\n" +
                "        \"password\": \"root\",\n" +
                "        \"database\": \"test_db\",\n" +
                "        \"query_pattern\": \"select surge_factor from customer where customer_id = %s;\",\n" +
                "        \"query_variables\": \"customer_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"idle_timeout\": \"5000\",\n" +
                "        \"fail_on_errors\": \"false\", \n" +
                "        \"capacity\": \"30\",\n" +
                "        \"type\": \"com.gojek.esb.aggregate.surge.SurgeFactorLogMessage\", \n" +
                "        \"output_mapping\": {\n" +
                "          \"surge_factor\": \"surge_factor\"   \n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "    \"internal_source\": [\n" +
                "       {" +
                "       \"output_field\": \"event_timestamp\", \n" +
                "       \"type\": \"function\",\n" +
                "       \"value\": \"CURRENT_TIMESTAMP\"\n" +
                "       }" +
                "   ]" +
                "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigWithInternalSourceString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.values.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();

        assertTrue(CollectSink.values.get(0).getField(1) instanceof Timestamp);
        assertEquals(23.33F, (Float) CollectSink.values.get(0).getField(0), 0.0F);
    }

    @Test
    public void shouldPopulateFieldFromPostgresOnSuccessResponseWithAllThreeSourcesIncludingTransformer() throws Exception {

        String postProcessorConfigWithTransformerString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"pg\": [\n" +
                "      {\n" +
                "        \"host\": \"" + host + "\",\n" +
                "        \"port\": \"5432\",\n" +
                "        \"user\": \"root\",\n" +
                "        \"password\": \"root\",\n" +
                "        \"database\": \"test_db\",\n" +
                "        \"query_pattern\": \"select surge_factor from customer where customer_id = %s;\",\n" +
                "        \"query_variables\": \"customer_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"idle_timeout\": \"5000\",\n" +
                "        \"fail_on_errors\": \"false\", \n" +
                "        \"capacity\": \"30\",\n" +
                "        \"type\": \"com.gojek.esb.aggregate.surge.SurgeFactorLogMessage\", \n" +
                "        \"output_mapping\": {\n" +
                "          \"surge_factor\": \"surge_factor\"   \n" +
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
                "       \"output_field\": \"customer_id\", \n" +
                "       \"type\": \"sql\",\n" +
                "       \"value\": \"customer_id\"\n" +
                "       }" +
                "   ]" +
                ",\n" +
                " \"transformers\": [" +
                "{\n" +
                "  \"transformation_class\": \"com.gojek.dagger.transformer.ClearColumnTransformer\",\n" +
                "  \"transformation_arguments\": {\n" +
                "    \"targetColumnName\": \"customer_id\"\n" +
                "  }\n" +
                "}\n" +
                "   ]   \n" +
                "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigWithTransformerString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.values.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());


        env.execute();
        assertEquals(23.33F, (Float) CollectSink.values.get(0).getField(0), 0.0F);
        assertEquals("", (String) CollectSink.values.get(0).getField(2));

        assertTrue(CollectSink.values.get(0).getField(1) instanceof Timestamp);
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

        static final List<Row> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Row inputRow, Context context) {
            values.add(inputRow);
        }
    }


    private static PgPool getPGClient() {

        String host = System.getenv("PG_HOST");
        if (StringUtils.isEmpty(host)) {
            host = "localhost";
        }

        if (pgClient == null) {


            PgConnectOptions connectOptions = new PgConnectOptions()
                    .setPort(5432)
                    .setHost(host)
                    .setDatabase("test_db")
                    .setUser("root")
                    .setPassword("root")
                    .setConnectTimeout(5000)
                    .setIdleTimeout(5000);

            PoolOptions poolOptions = new PoolOptions()
                    .setMaxSize(30);

            pgClient = pool(connectOptions, poolOptions);

        }
        return pgClient;

    }

}
