package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.async.decorator.async.HttpDecorator;
import com.gojek.daggers.postprocessor.parser.HttpExternalSourceConfig;
import com.gojek.de.stencil.StencilClient;
import com.gojek.esb.aggregate.surge.SurgeFactorLogMessage;
import com.jayway.jsonpath.InvalidJsonException;
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

    @Before
    public void setup() {
        initMocks(this);
        when(stencilClient.get("com.gojek.esb.aggregate.surge.SurgeFactorLogMessage")).thenReturn(SurgeFactorLogMessage.getDescriptor());
        when(httpDecorator.decorate(dataStream)).thenReturn(dataStream);
        when(configuration.getString(PORTAL_VERSION, "1")).thenReturn("1");
        when(configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "")).thenReturn("com.gojek.esb.aggregate.surge.SurgeFactorLog");
        when(configuration.getString(ASYNC_IO_CAPACITY_KEY, ASYNC_IO_CAPACITY_DEFAULT)).thenReturn(ASYNC_IO_CAPACITY_DEFAULT);
    }

    @Test
    public void shouldAddColumnNameToExistingColumnNamesOnTheBasisOfConfigGiven() {
        when(configuration.getString(EXTERNAL_SOURCE_KEY, "")).thenReturn("{\n" +
                "  \"http\": [\n" +
                "    {\n" +
                "      \"endpoint\": \"http://localhost:8000\",\n" +
                "      \"verb\": \"post\",\n" +
                "      \"body_column_from_sql\": \"request_body\",\n" +
                "      \"stream_timeout\": \"5000\",\n" +
                "      \"connect_timeout\": \"5000\",\n" +
                "      \"headers\": {\n" +
                "        \"content-type\": \"application/json\"\n" +
                "      },\n" +
                "      \"output_mapping\": {\n" +
                "        \"surge_factor\": {\n" +
                "          \"path\": \"$.surge\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}\n");

        String[] inputColumnNames = {"request_body", "order_number"};

        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        ExternalSourceProcessorMock externalSourceProcessorMock = new ExternalSourceProcessorMock(configuration, stencilClient, httpDecorator);
        StreamInfo result = externalSourceProcessorMock.process(streamInfo);
        String[] expectedOutputColumnNames = {"request_body", "order_number", "surge_factor"};
        Assert.assertEquals(true, Arrays.equals(expectedOutputColumnNames, result.getColumnNames()));
    }

    @Test
    public void shouldPassExistingColumnNamesIfNoColumnNameSpecifiedInConfig() {
        when(configuration.getString(EXTERNAL_SOURCE_KEY, "")).thenReturn("{\n" +
                "  \"http\": [\n" +
                "    {\n" +
                "      \"endpoint\": \"http://localhost:8000\",\n" +
                "      \"verb\": \"post\",\n" +
                "      \"body_column_from_sql\": \"request_body\",\n" +
                "      \"stream_timeout\": \"5000\",\n" +
                "      \"connect_timeout\": \"5000\",\n" +
                "      \"headers\": {\n" +
                "        \"content-type\": \"application/json\"\n" +
                "      },\n" +
                "      \"output_mapping\": {\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}\n");

        String[] inputColumnNames = {"request_body", "order_number"};

        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
        ExternalSourceProcessorMock externalSourceProcessorMock = new ExternalSourceProcessorMock(configuration, stencilClient, httpDecorator);
        StreamInfo result = externalSourceProcessorMock.process(streamInfo);
        String[] expectedOutputColumnNames = {"request_body", "order_number"};
        Assert.assertEquals(true, Arrays.equals(expectedOutputColumnNames, result.getColumnNames()));
    }

    @Test
    public void shouldAddMultipleColumnNamesToExistingColumnNamesOnTheBasisOfConfigGiven() {
        when(configuration.getString(EXTERNAL_SOURCE_KEY, "")).thenReturn("{\n" +
                "  \"http\": [\n" +
                "    {\n" +
                "      \"endpoint\": \"http://localhost:8000\",\n" +
                "      \"verb\": \"post\",\n" +
                "      \"body_column_from_sql\": \"request_body\",\n" +
                "      \"stream_timeout\": \"5000\",\n" +
                "      \"connect_timeout\": \"5000\",\n" +
                "      \"headers\": {\n" +
                "        \"content-type\": \"application/json\"\n" +
                "      },\n" +
                "      \"output_mapping\": {\n" +
                "        \"surge_factor\": {\n" +
                "          \"path\": \"$.surge\"\n" +
                "        },\n" +
                "        \"surge\": {\n" +
                "          \"path\": \"$.surge\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}\n");

        String[] inputColumnNames = {"request_body", "order_number"};

        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
        ExternalSourceProcessorMock externalSourceProcessorMock = new ExternalSourceProcessorMock(configuration, stencilClient, httpDecorator);
        StreamInfo result = externalSourceProcessorMock.process(streamInfo);
        String[] expectedOutputColumnNames = {"request_body", "order_number", "surge_factor", "surge"};
        Assert.assertEquals(true, Arrays.equals(expectedOutputColumnNames, result.getColumnNames()));
    }

    @Test
    public void shouldThrowExceptionIfExternalSourceKeyNotGiven() {
        expectedException.expect(InvalidJsonException.class);
        expectedException.expectMessage("Invalid JSON Given for EXTERNAL_SOURCE");

        when(configuration.getString(EXTERNAL_SOURCE_KEY, "")).thenReturn("test");
        String[] inputColumnNames = {"request_body", "order_number"};
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);
        ExternalSourceProcessorMock externalSourceProcessorMock = new ExternalSourceProcessorMock(configuration, stencilClient, httpDecorator);
        externalSourceProcessorMock.process(streamInfo);
    }

    @Test
    public void shouldReturnHttpDecorator() {
        ExternalSourceProcessor externalSourceProcessor = new ExternalSourceProcessor(configuration, stencilClient);
        String[] outputColumnNames = {"request_body", "order_number"};
        HttpDecorator httpDecorator = externalSourceProcessor.getHttpDecorator(outputColumnNames, "http", "bookingLog", httpExternalSourceConfig, 40);
        Assert.assertEquals("40", httpDecorator.getAsyncIOCapacity().toString());
    }

    class ExternalSourceProcessorMock extends ExternalSourceProcessor {

        private HttpDecorator mockHttpDecorator;

        public ExternalSourceProcessorMock(Configuration configuration, StencilClient stencilClient, HttpDecorator mockHttpDecorator) {
            super(configuration, stencilClient);
            this.mockHttpDecorator = mockHttpDecorator;
        }

        protected HttpDecorator getHttpDecorator(String[] outputColumnNames, String type, String outputProto, HttpExternalSourceConfig httpExternalSourceConfig, Integer asyncIOCapacity) {
            return this.mockHttpDecorator;
        }
    }

}