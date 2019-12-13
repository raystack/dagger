package com.gojek.daggers.postProcessors.external;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import com.gojek.daggers.postProcessors.external.es.EsStreamDecorator;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import com.gojek.daggers.postProcessors.external.http.HttpStreamDecorator;
import com.gojek.de.stencil.StencilClient;
import com.gojek.esb.aggregate.surge.SurgeFactorLogMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.*;

import static com.gojek.daggers.utils.Constants.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class ExternalPostProcessorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private Configuration configuration;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private DataStream dataStream;

    @Mock
    private HttpStreamDecorator httpStreamDecorator;

    @Mock
    private EsStreamDecorator esStreamDecorator;

    @Mock
    private HttpSourceConfig httpSourceConfig;

    @Mock
    private TelemetrySubscriber telemetrySubscriber;

//    private ExternalPostProcessorMock externalSourceProcessorMock;

    private PostProcessorConfig postProcessorConfig;
    private EsSourceConfig esSourceConfig;
    private ExternalSourceConfig externalSourceConfig;
    private ColumnNameManager columnNameManager;
    private ExternalPostProcessor externalPostProcessor;

    @Before
    public void setup() {
        initMocks(this);
        HashMap<String, OutputMapping> httpColumnNames = new HashMap<>();
        httpColumnNames.put("http_field_1", new OutputMapping(""));
        httpColumnNames.put("http_field_2", new OutputMapping(""));
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("endpoint", "POST", "/some/patttern/%s", "variable", "123", "234", false, "type", "20", new HashMap<>(), httpColumnNames);
        HashMap<String, OutputMapping> esOutputMapping = new HashMap<>();
        esOutputMapping.put("es_field_1", new OutputMapping(""));
        String[] inputColumnNames = {"http_input_field_1", "http_input_field_2", "http_input_field_3"};
        List<String> outputColumnNames = Arrays.asList("http_field_1", "http_field_2");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        EsSourceConfig esSourceConfig = new EsSourceConfig("host", "port", "endpointPattern", "endpointVariable", "type", "30", "123", "234", "345", "456", false, new HashMap<>());
        externalSourceConfig = new ExternalSourceConfig(Arrays.asList(httpSourceConfig), Arrays.asList(esSourceConfig));
        when(stencilClient.get("com.gojek.esb.aggregate.surge.SurgeFactorLogMessage")).thenReturn(SurgeFactorLogMessage.getDescriptor());
        when(httpStreamDecorator.decorate(dataStream)).thenReturn(dataStream);
        when(configuration.getString(PORTAL_VERSION, "1")).thenReturn("1");
        when(configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "")).thenReturn("com.gojek.esb.aggregate.surge.SurgeFactorLog");
        when(configuration.getString(ASYNC_IO_CAPACITY_KEY, ASYNC_IO_CAPACITY_DEFAULT)).thenReturn(ASYNC_IO_CAPACITY_DEFAULT);

        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"http\": [\n" +
                "      {\n" +
                "        \"endpoint\": \"http://10.202.120.225/seldon/mlp-showcase/integrationtest/api/v0.1/predictions\",\n" +
                "        \"verb\": \"post\",\n" +
                "        \"body_pattern\": \"{'data':{'names': ['s2id'], 'tensor': {'shape': [1,1], 'values':[%s]}}}\",\n" +
                "        \"body_variables\": \"s2_id\",\n" +
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
                "            \"path\": \"$.data.tensor.values[0]\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        postProcessorConfig = PostProcessorConfig.parse(postProcessorConfigString);
//        externalPostProcessor = new ExternalPostProcessorMock(stencilClient, externalSourceConfig, columnNameManager,httpStreamDecorator,esStreamDecorator);
        externalPostProcessor = new ExternalPostProcessor(stencilClient, externalSourceConfig, columnNameManager, telemetrySubscriber);
    }

    @Test
    public void shouldBeTrueWhenExternalSourceExists() {
        assertTrue(externalPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeFalseWhenExternalSourceDoesNotExist() {
        postProcessorConfig = new PostProcessorConfig(null, null, null);

        assertFalse(externalPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldProcessWithRightConfiguration() {
        Map<String, OutputMapping> outputMapping = new HashMap<>();
        outputMapping.put("order_id", new OutputMapping("path"));

        List<HttpSourceConfig> httpSourceConfigs = new ArrayList<>();
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("endpoint", "POST", "/some/patttern/%s", "variable", "123", "234", false, "type", "20", new HashMap<>(), outputMapping);
        httpSourceConfigs.add(httpSourceConfig);

        List<EsSourceConfig> esSourceConfigs = new ArrayList<>();
        EsSourceConfig esSourceConfig = new EsSourceConfig("host", "1000", "/some/pattern/%s", "variable", "type", "20", "111", "222", "100", "200", false, outputMapping);
        esSourceConfigs.add(esSourceConfig);

        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(httpSourceConfigs, esSourceConfigs);

        StreamInfo streamInfoMock = mock(StreamInfo.class);
        HttpStreamDecorator httpDecoratorMock = mock(HttpStreamDecorator.class);
        EsStreamDecorator esDecoratorMock = mock(EsStreamDecorator.class);
        ExternalPostProcessorMock externalPostProcessorMock = new ExternalPostProcessorMock(stencilClient, externalSourceConfig, columnNameManager, httpDecoratorMock, esDecoratorMock);

        externalPostProcessorMock.process(streamInfoMock);
    }

//    @Test
//    public void shouldAddColumnNameToExistingColumnNamesOnTheBasisOfConfigGiven() {
//        String[] inputColumnNames = {"request_body", "order_number"};
//
//        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
//
//        StreamInfo result = externalPostProcessor.process(streamInfo);
//        String[] expectedOutputColumnNames = {"request_body", "order_number", "surge_factor"};
////        Assert.assertEquals(true, Arrays.equals(expectedOutputColumnNames, result.getColumnNames()));
//    }

    @Test
    public void shouldPassExistingColumnNamesIfNoColumnNameSpecifiedInConfig() {
        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"http\": [\n" +
                "      {\n" +
                "        \"endpoint\": \"http://localhost:8000\",\n" +
                "        \"verb\": \"post\",\n" +
                "        \"body_column_from_sql\": \"request_body\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"fail_on_errors\": \"true\", \n" +
                "        \"headers\": {\n" +
                "          \"content-type\": \"application/json\"\n" +
                "        },\n" +
                "        \"output_mapping\": {\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(postProcessorConfigString);
//        externalSourceProcessorMock = new ExternalPostProcessorMock(configuration, stencilClient, httpStreamDecorator, postProcessorConfig);

        String[] inputColumnNames = {"request_body", "order_number"};

        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

//        StreamInfo result = externalSourceProcessorMock.process(streamInfo);
        String[] expectedOutputColumnNames = {"request_body", "order_number"};
//        Assert.assertEquals(true, Arrays.equals(expectedOutputColumnNames, result.getColumnNames()));
    }

    @Test
    public void shouldAddMultipleColumnNamesToExistingColumnNamesOnTheBasisOfConfigGiven() {
        String postProcessorConfigString = "{\n" +
                "  \"external_source\": {\n" +
                "    \"http\": [\n" +
                "      {\n" +
                "        \"endpoint\": \"http://localhost:8000\",\n" +
                "        \"verb\": \"post\",\n" +
                "        \"body_column_from_sql\": \"request_body\",\n" +
                "        \"stream_timeout\": \"5000\",\n" +
                "        \"connect_timeout\": \"5000\",\n" +
                "        \"fail_on_errors\": \"true\", \n" +
                "        \"headers\": {\n" +
                "          \"content-type\": \"application/json\"\n" +
                "        },\n" +
                "        \"output_mapping\": {\n" +
                "        \"surge_factor\": {\n" +
                "          \"path\": \"$.surge\"\n" +
                "        },\n" +
                "        \"surge\": {\n" +
                "          \"path\": \"$.surge\"\n" +
                "        }\n" +
                "      }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(postProcessorConfigString);
//        externalSourceProcessorMock = new ExternalPostProcessorMock(configuration, stencilClient, httpStreamDecorator, postProcessorConfig);
        String[] inputColumnNames = {"request_body", "order_number"};

        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
//        StreamInfo result = externalSourceProcessorMock.process(streamInfo);
        String[] expectedOutputColumnNames = {"request_body", "order_number", "surge_factor", "surge"};
//        Assert.assertEquals(true, Arrays.equals(expectedOutputColumnNames, result.getColumnNames()));
    }


    @Test
    public void shouldReturnHttpDecorator() {
//        ExternalPostProcessor externalPostProcessor = new ExternalPostProcessor(stencilClient, postProcessorConfig, columnNameManager);
        String[] outputColumnNames = {"request_body", "order_number"};
        Row outputRow = new Row(2);
//        HttpStreamDecorator httpStreamDecorator = externalPostProcessor.getHttpDecorator(outputRow, "http", 40);
//        Assert.assertEquals("40", httpStreamDecorator.getAsyncIOCapacity().toString());
    }

    @Test
    public void shouldReturnEsStreamDecorator() {
//        ExternalPostProcessor externalPostProcessor = new ExternalPostProcessor(stencilClient, postProcessorConfig, columnNameManager);
        String[] outputColumnNames = {};
//        EsStreamDecorator esStreamDecorator = externalPostProcessor.getEsStreamDecorator(esSourceConfig, outputColumnNames, inputColumnNames);
//        Assert.assertEquals("40", esStreamDecorator.getAsyncIOCapacity().toString());
    }

    class ExternalPostProcessorMock extends ExternalPostProcessor {

        private HttpStreamDecorator httpStreamDecorator;
        private EsStreamDecorator esStreamDecorator;

        public ExternalPostProcessorMock(StencilClient stencilClient, ExternalSourceConfig externalSourceConfig, ColumnNameManager columnNameManager, HttpStreamDecorator httpStreamDecorator, EsStreamDecorator esStreamDecorator) {
            super(stencilClient, externalSourceConfig, columnNameManager, telemetrySubscriber);
            this.httpStreamDecorator = httpStreamDecorator;
            this.esStreamDecorator = esStreamDecorator;
        }

        @Override
        protected HttpStreamDecorator getHttpDecorator(HttpSourceConfig httpSourceConfig, ColumnNameManager columnNameManager, TelemetrySubscriber telemetrySubscriber) {
            return httpStreamDecorator;
        }

        @Override
        protected EsStreamDecorator getEsDecorator(EsSourceConfig esSourceConfig, ColumnNameManager columnNameManager, TelemetrySubscriber telemetrySubscriber) {
            return esStreamDecorator;
        }
    }

}