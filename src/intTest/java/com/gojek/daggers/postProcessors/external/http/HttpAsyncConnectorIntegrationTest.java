package com.gojek.daggers.postProcessors.external.http;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.FetchOutputDecorator;
import com.gojek.daggers.postProcessors.external.common.InitializationDecorator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;

public class HttpAsyncConnectorIntegrationTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089);

    private ColumnNameManager columnNameManager;
    private boolean telemetryEnabled;
    private long shutDownPeriod;
    private HttpSourceConfig httpSourceConfig;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Before
    public void setUp() {
        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        telemetryEnabled = false;
        shutDownPeriod = 0L;
        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"http\": [\n" +
                "      {\n" +
                "        \"endpoint\": \"http://localhost:8089\",\n" +
                "        \"verb\": \"get\",\n" +
                "        \"request_pattern\": \"/customer/%s\",\n" +
                "        \"request_variables\": \"customer_id\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"fail_on_errors\": \"false\", \n" +
                "        \"capacity\": \"30\",\n" +
                "        \"headers\": {\n" +
                "          \"content-type\": \"application/json\"\n" +
                "        },\n" +
                "        \"type\": \"com.gojek.esb.aggregate.surge.SurgeFactorLogMessage\", \n" +
                "        \"output_mapping\": {\n" +
                "          \"surge_factor\": {\n" +
                "            \"path\": \"$.data\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
        PostProcessorConfig postProcessorParsedConfig = PostProcessorConfig.parse(postProcessorConfigString);
        httpSourceConfig = postProcessorParsedConfig.getExternalSource().getHttpConfig().get(0);
        columnNameManager = new ColumnNameManager(inputColumnNames, postProcessorParsedConfig.getOutputColumnNames());
    }

    @After
    public void tearDown() {
        wireMockRule.stop();
    }

    @Test
    public void shouldPopulateFieldFromHTTPGetApiOnSuccessResponseWithCorrespondingDataTypeIfTypeGiven() throws Exception {

        stubFor(get(urlEqualTo("/customer/123"))
                .withHeader("content-type", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{data: 23.33}")));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.values.clear();

        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData).map(new InitializationDecorator(columnNameManager));

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, "", new StencilClientOrchestrator(new Configuration()),
                columnNameManager, null, telemetryEnabled, shutDownPeriod, null, null);

        DataStream<Row> outputStream = AsyncDataStream.orderedWait(dataStream, httpAsyncConnector, httpSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, httpSourceConfig.getCapacity());

        outputStream.map(new FetchOutputDecorator()).addSink(new CollectSink());
        env.execute();
        assertEquals(23.33F, (Float) CollectSink.values.get(0).getField(0), 0.0F);
    }

    private static class CollectSink implements SinkFunction<Row> {

        static final List<Row> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Row inputRow, Context context) {
            values.add(inputRow);
        }
    }
}