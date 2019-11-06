package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.async.decorator.async.HttpDecorator;
import com.gojek.daggers.postprocessor.parser.HttpExternalSourceConfig;
import com.gojek.daggers.postprocessor.parser.PostProcessorConfig;
import com.gojek.de.stencil.StencilClient;
import com.gojek.esb.aggregate.surge.SurgeFactorLogMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Arrays;

import static com.gojek.daggers.Constants.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class ExternalSourceProcessorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private Configuration configuration;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private DataStream dataStream;

    @Mock
    private HttpDecorator httpDecorator;

    @Mock
    private HttpExternalSourceConfig httpExternalSourceConfig;

    private ExternalSourceProcessorMock externalSourceProcessorMock;

    private PostProcessorConfig postProcessorConfig;

    @Before
    public void setup() {
        initMocks(this);
        when(stencilClient.get("com.gojek.esb.aggregate.surge.SurgeFactorLogMessage")).thenReturn(SurgeFactorLogMessage.getDescriptor());
        when(httpDecorator.decorate(dataStream)).thenReturn(dataStream);
        when(configuration.getString(PORTAL_VERSION, "1")).thenReturn("1");
        when(configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "")).thenReturn("com.gojek.esb.aggregate.surge.SurgeFactorLog");
        when(configuration.getString(ASYNC_IO_CAPACITY_KEY, ASYNC_IO_CAPACITY_DEFAULT)).thenReturn(ASYNC_IO_CAPACITY_DEFAULT);

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
                "          \"surge_factor\": {\n" +
                "            \"path\": \"$.data.tensor.values[0]\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  } \n" +
                "}";

        postProcessorConfig = PostProcessorConfig.parse(postProcessorConfigString);
        externalSourceProcessorMock = new ExternalSourceProcessorMock(configuration, stencilClient, httpDecorator, postProcessorConfig);
    }

    @Test
    public void shouldAddColumnNameToExistingColumnNamesOnTheBasisOfConfigGiven() {
        String[] inputColumnNames = {"request_body", "order_number"};

        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo result = externalSourceProcessorMock.process(streamInfo);
        String[] expectedOutputColumnNames = {"request_body", "order_number", "surge_factor"};
        Assert.assertEquals(true, Arrays.equals(expectedOutputColumnNames, result.getColumnNames()));
    }

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
        externalSourceProcessorMock = new ExternalSourceProcessorMock(configuration, stencilClient, httpDecorator, postProcessorConfig);

        String[] inputColumnNames = {"request_body", "order_number"};

        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo result = externalSourceProcessorMock.process(streamInfo);
        String[] expectedOutputColumnNames = {"request_body", "order_number"};
        Assert.assertEquals(true, Arrays.equals(expectedOutputColumnNames, result.getColumnNames()));
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
        externalSourceProcessorMock = new ExternalSourceProcessorMock(configuration, stencilClient, httpDecorator, postProcessorConfig);
        String[] inputColumnNames = {"request_body", "order_number"};

        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
        StreamInfo result = externalSourceProcessorMock.process(streamInfo);
        String[] expectedOutputColumnNames = {"request_body", "order_number", "surge_factor", "surge"};
        Assert.assertEquals(true, Arrays.equals(expectedOutputColumnNames, result.getColumnNames()));
    }


    @Test
    public void shouldReturnHttpDecorator() {
        ExternalSourceProcessor externalSourceProcessor = new ExternalSourceProcessor(configuration, stencilClient, postProcessorConfig);
        String[] outputColumnNames = {"request_body", "order_number"};
        HttpDecorator httpDecorator = externalSourceProcessor.getHttpDecorator(outputColumnNames, "http", httpExternalSourceConfig, 40);
        Assert.assertEquals("40", httpDecorator.getAsyncIOCapacity().toString());
    }

    class ExternalSourceProcessorMock extends ExternalSourceProcessor {

        private HttpDecorator mockHttpDecorator;

        public ExternalSourceProcessorMock(Configuration configuration, StencilClient stencilClient, HttpDecorator mockHttpDecorator, PostProcessorConfig postProcessorConfig) {
            super(configuration, stencilClient, postProcessorConfig);
            this.mockHttpDecorator = mockHttpDecorator;
        }

        protected HttpDecorator getHttpDecorator(String[] outputColumnNames, String type, HttpExternalSourceConfig httpExternalSourceConfig, Integer asyncIOCapacity) {
            return this.mockHttpDecorator;
        }
    }

}