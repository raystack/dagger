package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.async.decorator.async.HttpDecorator;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getBoolean(EXTERNAL_SOURCE_ENABLED_KEY, EXTERNAL_SOURCE_ENABLED_KEY_DEFAULT)).thenReturn(true);
        when(configuration.getString(ASYNC_IO_CAPACITY_KEY, ASYNC_IO_CAPACITY_DEFAULT)).thenReturn("30");
        when(configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "")).thenReturn("com.gojek.esb.aggregate.surge.SurgeFactorLog");
        when(stencilClient.get("com.gojek.esb.aggregate.surge.SurgeFactorLogMessage")).thenReturn(SurgeFactorLogMessage.getDescriptor());
        when(configuration.getString(eq("PORTAL_VERSION"), anyString())).thenReturn("1");
        when(httpDecorator.decorate(dataStream)).thenReturn(dataStream);
    }

    @Test
    public void shouldAddColumnNameToExistingColumnNamesOnTheBasisOfConfigGiven() {
        when(configuration.getString(EXTERNAL_SOURCE_KEY, "")).thenReturn("{\n" +
                "  \"http\": [\n" +
                "    {\n" +
                "      \"endpoint\": \"http://localhost:8000\",\n" +
                "      \"verb\": \"post\",\n" +
                "      \"body_field\": \"request_body\",\n" +
                "      \"stream_timeout\": \"5000\",\n" +
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
        ExternalSourceProcessor externalSourceProcessor = new ExternalSourceProcessor(configuration, stencilClient, httpDecorator);
        StreamInfo result = externalSourceProcessor.process(streamInfo);
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
                "      \"body_field\": \"request_body\",\n" +
                "      \"stream_timeout\": \"5000\",\n" +
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
        ExternalSourceProcessor externalSourceProcessor = new ExternalSourceProcessor(configuration, stencilClient, httpDecorator);
        StreamInfo result = externalSourceProcessor.process(streamInfo);
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
                "      \"body_field\": \"request_body\",\n" +
                "      \"stream_timeout\": \"5000\",\n" +
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
        ExternalSourceProcessor externalSourceProcessor = new ExternalSourceProcessor(configuration, stencilClient, httpDecorator);
        StreamInfo result = externalSourceProcessor.process(streamInfo);
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
        ExternalSourceProcessor externalSourceProcessor = new ExternalSourceProcessor(configuration, stencilClient, httpDecorator);
        externalSourceProcessor.process(streamInfo);
    }

}