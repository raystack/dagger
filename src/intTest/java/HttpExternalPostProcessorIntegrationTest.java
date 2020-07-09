//import com.github.tomakehurst.wiremock.junit.WireMockRule;
//import com.gojek.daggers.core.StencilClientOrchestrator;
//import com.gojek.daggers.core.StreamInfo;
//import com.gojek.daggers.postProcessors.PostProcessorFactory;
//import com.gojek.daggers.postProcessors.common.PostProcessor;
//import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
//import com.gojek.daggers.utils.Constants;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.test.util.MiniClusterWithClientResource;
//import org.apache.flink.types.Row;
//import org.junit.*;
//
//import java.sql.Timestamp;
//import java.util.ArrayList;
//import java.util.List;
//
//import static com.github.tomakehurst.wiremock.client.WireMock.*;
//import static com.gojek.daggers.utils.Constants.POST_PROCESSOR_ENABLED_KEY;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//public class HttpExternalPostProcessorIntegrationTest {
//
//    @Rule
//    public WireMockRule wireMockRule = new WireMockRule(8089);
//
//    private StencilClientOrchestrator stencilClientOrchestrator;
//    private MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();
//    private Configuration configuration = new Configuration();
//
//    @ClassRule
//    public static MiniClusterWithClientResource flinkCluster =
//            new MiniClusterWithClientResource(
//                    new MiniClusterResourceConfiguration.Builder()
//                            .setNumberSlotsPerTaskManager(1)
//                            .setNumberTaskManagers(1)
//                            .build());
//
//    @Before
//    public void setUp() {
//        configuration.setString(POST_PROCESSOR_ENABLED_KEY, "true");
//    }
//
//    @After
//    public void tearDown() {
//        wireMockRule.stop();
//    }
//
//    @Test
//    public void shouldPopulateFieldFromHTTPGetApiOnSuccessResponseWithCorrespondingDataTypeIfTypeGiven() throws Exception {
//        String postProcessorConfigString = "{\n" +
//                "  \"external_source\": {\n" +
//                "    \"http\": [\n" +
//                "      {\n" +
//                "        \"endpoint\": \"http://localhost:8089\",\n" +
//                "        \"verb\": \"get\",\n" +
//                "        \"request_pattern\": \"/customer/%s\",\n" +
//                "        \"request_variables\": \"customer_id\",\n" +
//                "        \"stream_timeout\": \"5000\",\n" +
//                "        \"connect_timeout\": \"5000\",\n" +
//                "        \"fail_on_errors\": \"false\", \n" +
//                "        \"capacity\": \"30\",\n" +
//                "        \"headers\": {\n" +
//                "          \"content-type\": \"application/json\"\n" +
//                "        },\n" +
//                "        \"type\": \"com.gojek.esb.aggregate.surge.SurgeFactorLogMessage\", \n" +
//                "        \"output_mapping\": {\n" +
//                "          \"surge_factor\": {\n" +
//                "            \"path\": \"$.data\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      }\n" +
//                "    ]\n" +
//                "  }\n" +
//                "}";
//
//        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
//        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
//
//        stubFor(get(urlEqualTo("/customer/123"))
//                .withHeader("content-type", equalTo("application/json"))
//                .willReturn(aResponse()
//                        .withStatus(200)
//                        .withHeader("Content-Type", "application/json")
//                        .withBody("{data: 23.33}")));
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        CollectSink.values.clear();
//
//        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
//        Row inputData = new Row(3);
//        inputData.setField(0, "RB-123");
//        inputData.setField(1, "123");
//        inputData.setField(2, "456");
//
//        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
//        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
//
//        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
//        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());
//
//        env.execute();
//        assertEquals(23.33F, (Float) CollectSink.values.get(0).getField(0), 0.0F);
//    }
//
//    @Test
//    public void shouldPopulateFieldFromHTTPGetApiOnSuccessResponseWithCorrespondingDataTypeIfTypeNotGiven() throws Exception {
//        String postProcessorConfigString = "{\n" +
//                "  \"external_source\": {\n" +
//                "    \"http\": [\n" +
//                "      {\n" +
//                "        \"endpoint\": \"http://localhost:8089\",\n" +
//                "        \"verb\": \"get\",\n" +
//                "        \"request_pattern\": \"/customer/%s\",\n" +
//                "        \"request_variables\": \"customer_id\",\n" +
//                "        \"stream_timeout\": \"5000\",\n" +
//                "        \"connect_timeout\": \"5000\",\n" +
//                "        \"fail_on_errors\": \"false\", \n" +
//                "        \"capacity\": \"30\",\n" +
//                "        \"headers\": {\n" +
//                "          \"content-type\": \"application/json\"\n" +
//                "        },\n" +
//                "        \"output_mapping\": {\n" +
//                "          \"surge_factor\": {\n" +
//                "            \"path\": \"$.data\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      }\n" +
//                "    ]\n" +
//                "  }\n" +
//                "}";
//
//        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
//        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
//
//        stubFor(get(urlEqualTo("/customer/123"))
//                .withHeader("content-type", equalTo("application/json"))
//                .willReturn(aResponse()
//                        .withStatus(200)
//                        .withHeader("Content-Type", "application/json")
//                        .withBody("{data: 23.33}")));
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        CollectSink.values.clear();
//
//        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
//        Row inputData = new Row(3);
//        inputData.setField(0, "RB-123");
//        inputData.setField(1, "123");
//        inputData.setField(2, "456");
//
//        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
//        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
//
//        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
//        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());
//
//        env.execute();
//        assertEquals(23.33, (Double) CollectSink.values.get(0).getField(0), 0.0);
//    }
//
//    @Test
//    public void shouldPopulateFieldFromHTTPGetApiOnSuccessResponseWithExternalAndInternalSource() throws Exception {
//        String postProcessorConfigWithInternalSourceString = "{\n" +
//                "  \"external_source\": {\n" +
//                "    \"http\": [\n" +
//                "      {\n" +
//                "        \"endpoint\": \"http://localhost:8089\",\n" +
//                "        \"verb\": \"get\",\n" +
//                "        \"request_pattern\": \"/customer/%s\",\n" +
//                "        \"request_variables\": \"customer_id\",\n" +
//                "        \"stream_timeout\": \"5000\",\n" +
//                "        \"connect_timeout\": \"5000\",\n" +
//                "        \"fail_on_errors\": \"false\", \n" +
//                "        \"capacity\": \"30\",\n" +
//                "        \"headers\": {\n" +
//                "          \"content-type\": \"application/json\"\n" +
//                "        },\n" +
//                "        \"type\": \"com.gojek.esb.aggregate.surge.SurgeFactorLogMessage\", \n" +
//                "        \"output_mapping\": {\n" +
//                "          \"surge_factor\": {\n" +
//                "            \"path\": \"$.data\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      }\n" +
//                "    ]\n" +
//                "  },\n" +
//                "    \"internal_source\": [\n" +
//                "       {" +
//                "       \"output_field\": \"event_timestamp\", \n" +
//                "       \"type\": \"function\",\n" +
//                "       \"value\": \"CURRENT_TIMESTAMP\"\n" +
//                "       }" +
//                "   ]" +
//                "}";
//
//        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigWithInternalSourceString);
//        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
//
//        stubFor(get(urlEqualTo("/customer/123"))
//                .withHeader("content-type", equalTo("application/json"))
//                .willReturn(aResponse()
//                        .withStatus(200)
//                        .withHeader("Content-Type", "application/json")
//                        .withBody("{data: 23.33}")));
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        CollectSink.values.clear();
//
//        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
//        Row inputData = new Row(3);
//        inputData.setField(0, "RB-123");
//        inputData.setField(1, "123");
//        inputData.setField(2, "456");
//
//        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
//        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
//
//        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
//        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());
//
//        env.execute();
//        assertEquals(23.33F, (Float) CollectSink.values.get(0).getField(0), 0.0F);
//        assertTrue(CollectSink.values.get(0).getField(1) instanceof Timestamp);
//    }
//
//    @Test
//    public void shouldPopulateFieldFromHTTPGetApiOnSuccessResponseWithAllThreeSourcesIncludingTransformer() throws Exception {
//        String postProcessorConfigWithTransformerString = "{\n" +
//                "  \"external_source\": {\n" +
//                "    \"http\": [\n" +
//                "      {\n" +
//                "        \"endpoint\": \"http://localhost:8089\",\n" +
//                "        \"verb\": \"get\",\n" +
//                "        \"request_pattern\": \"/customer/%s\",\n" +
//                "        \"request_variables\": \"customer_id\",\n" +
//                "        \"stream_timeout\": \"5000\",\n" +
//                "        \"connect_timeout\": \"5000\",\n" +
//                "        \"fail_on_errors\": \"false\", \n" +
//                "        \"capacity\": \"30\",\n" +
//                "        \"headers\": {\n" +
//                "          \"content-type\": \"application/json\"\n" +
//                "        },\n" +
//                "        \"output_mapping\": {\n" +
//                "          \"features\": {\n" +
//                "            \"path\": \"$.data\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      }\n" +
//                "    ]\n" +
//                "  },\n" +
//                "    \"internal_source\": [\n" +
//                "       {" +
//                "       \"output_field\": \"event_timestamp\", \n" +
//                "       \"type\": \"function\",\n" +
//                "       \"value\": \"CURRENT_TIMESTAMP\"\n" +
//                "       }," +
//                "       {" +
//                "       \"output_field\": \"customer_id\", \n" +
//                "       \"type\": \"sql\",\n" +
//                "       \"value\": \"customer_id\"\n" +
//                "       }" +
//                "   ]" +
//                ",\n" +
//                " \"transformers\": [" +
//                "    {" +
//                "   \"transformation_arguments\": { \n" +
//                "   \"keyColumnName\": \"customer_id\", \n" +
//                "    \"valueColumnName\": \"features\" \n" +
//                "   }," +
//                "   \"transformation_class\": \"com.gojek.dagger.transformer.FeatureTransformer\" \n" +
//                "   } \n" +
//                "   ]   \n" +
//                "}";
//
//        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigWithTransformerString);
//        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
//
//        stubFor(get(urlEqualTo("/customer/123"))
//                .withHeader("content-type", equalTo("application/json"))
//                .willReturn(aResponse()
//                        .withStatus(200)
//                        .withHeader("Content-Type", "application/json")
//                        .withBody("{data: 23.33}")));
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        CollectSink.values.clear();
//
//        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
//        Row inputData = new Row(3);
//        inputData.setField(0, "RB-123");
//        inputData.setField(1, "123");
//        inputData.setField(2, "456");
//
//        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
//        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
//
//        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
//        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());
//
//        env.execute();
//        assertEquals(23.33, ((Row) ((Row[]) CollectSink.values.get(0).getField(0))[0].getField(1)).getField(4));
//        assertTrue(CollectSink.values.get(0).getField(1) instanceof Timestamp);
//    }
//
//    private static class CollectSink implements SinkFunction<Row> {
//
//        static final List<Row> values = new ArrayList<>();
//
//        @Override
//        public synchronized void invoke(Row inputRow, Context context) {
//            values.add(inputRow);
//        }
//    }
//
//    private StreamInfo addPostProcessor(StreamInfo streamInfo) {
//        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, streamInfo.getColumnNames(), telemetryExporter);
//        StreamInfo postProcessedStream = streamInfo;
//        for (PostProcessor postProcessor : postProcessors) {
//            postProcessedStream = postProcessor.process(postProcessedStream);
//        }
//        return postProcessedStream;
//    }
//}