package com.gojek.daggers.async.connector;

import com.gojek.daggers.utils.stats.StatsManager;
import com.gojek.esb.aggregate.surge.SurgeFactorLogMessage;
import com.google.protobuf.Descriptors;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.Response;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.gojek.daggers.Constants.*;
import static com.gojek.daggers.async.metric.ExternalSourceAspects.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpResponseHandlerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private ResultFuture<Row> resultFuture;

    @Mock
    private Response response;

    @Mock
    private StatsManager statsManager;

    private Row outputRow;
    private Map<String, Object> configuration = new HashMap<>();
    private String[] columnNames;
    private Descriptors.Descriptor descriptor;

    @Before
    public void setup() {
        initMocks(this);
        outputRow = new Row(1);
        descriptor = SurgeFactorLogMessage.getDescriptor();
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIs4XX() throws Exception {
        configuration.put(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, EXTERNAL_SOURCE_FAIL_ON_ERRORS_DEFAULT);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(404);
        httpResponseHandler.onCompleted(response);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputRow));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_4XX);
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIs5XX() throws Exception {
        configuration.put(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, EXTERNAL_SOURCE_FAIL_ON_ERRORS_DEFAULT);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(502);
        httpResponseHandler.onCompleted(response);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputRow));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_5XX);
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIsOtherThan5XXAnd4XX() throws Exception {
        configuration.put(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, EXTERNAL_SOURCE_FAIL_ON_ERRORS_DEFAULT);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(302);
        httpResponseHandler.onCompleted(response);
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_STATUS);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputRow));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIs4XX() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Received status code : 404");

        configuration.put(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, true);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(404);
        httpResponseHandler.onCompleted(response);
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_4XX);
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIs5XX() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Received status code : 502");

        configuration.put(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, true);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(502);
        httpResponseHandler.onCompleted(response);
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_5XX);
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIsOtherThan5XXAnd4XX() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Received status code : 302");

        configuration.put(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, true);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(302);
        httpResponseHandler.onCompleted(response);
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_STATUS);
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndOnThrowable() throws Exception {
        configuration.put(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, EXTERNAL_SOURCE_FAIL_ON_ERRORS_DEFAULT);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        Throwable throwable = new Throwable("throwable message");
        httpResponseHandler.onThrowable(throwable);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputRow));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_ERRORS);
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndOnThrowable() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("throwable message");

        configuration.put(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, true);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        Throwable throwable = new Throwable("throwable message");
        httpResponseHandler.onThrowable(throwable);
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_ERRORS);
    }

    @Test
    public void shouldThrowExceptionIfOutputMappingIsNotMap() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY + " config should be passed as a map");

        configuration.put(EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY, "string");
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(200);
        httpResponseHandler.onCompleted(response);
    }

    @Test
    public void shouldThrowExceptionIfOutputMappingIsNotGiven() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY + " config should not be empty");

        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(200);
        httpResponseHandler.onCompleted(response);
    }


    @Test
    public void shouldThrowExceptionIfFieldsOfOutputMappingIsNotMap() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("surge_factor config should be passed as a map");

        Map<String, Object> outputMapping = new HashMap<>();
        outputMapping.put("surge_factor", "test");
        configuration.put(EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY, outputMapping);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(200);
        httpResponseHandler.onCompleted(response);
    }

    @Test
    public void shouldThrowExceptionIfOutputMappingFieldIsNotGiven() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("surge_factor config should not be empty");

        Map<String, Object> outputMapping = new HashMap<>();
        outputMapping.put("surge_factor", null);
        configuration.put(EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY, outputMapping);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(200);
        httpResponseHandler.onCompleted(response);
    }

    @Test
    public void shouldPopulateSingleResultFromHttpCallInInputRow() throws Exception {
        configuration.put(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY, "http://localhost");
        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");
        HashMap<String, Object> outputMappings = new HashMap<>();
        HashMap<String, String> field1Config = new HashMap<>();
        field1Config.put("path", "$.surge");
        outputMappings.put("surge_factor", field1Config);
        configuration.put(EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY, outputMappings);


        Row inputRow = new Row(2);
        inputRow.setField(0, "body");
        Row resultRow = new Row(2);
        resultRow.setField(0, "body");
        resultRow.setField(1, 0.732f);
        columnNames = new String[]{"request_body", "surge_factor"};

        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(inputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732\n" +
                "}");
        httpResponseHandler.onCompleted(response);
        verify(statsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(resultFuture, times(1)).complete(Collections.singleton(resultRow));
    }

    @Test
    public void shouldPopulateMultipleResultsFromHttpCallInInputRow() throws Exception {
        configuration.put(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY, "http://localhost");
        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");
        HashMap<String, Object> outputMappings = new HashMap<>();
        HashMap<String, String> field1Config = new HashMap<>();
        field1Config.put("path", "$.surge");
        HashMap<String, String> field2Config = new HashMap<>();
        field2Config.put("path", "$.prediction");
        outputMappings.put("surge_factor", field1Config);
        outputMappings.put("s2_id_level", field2Config);
        configuration.put(EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY, outputMappings);


        Row inputRow = new Row(3);
        inputRow.setField(0, "body");
        Row resultRow = new Row(3);
        resultRow.setField(0, "body");
        resultRow.setField(1, 0.732f);
        resultRow.setField(2, 345);
        columnNames = new String[]{"request_body", "surge_factor", "s2_id_level"};

        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(inputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732,\n" +
                "  \"prediction\": 345\n" +
                "}");
        httpResponseHandler.onCompleted(response);
        verify(statsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(resultFuture, times(1)).complete(Collections.singleton(resultRow));
    }

}
