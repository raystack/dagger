package com.gojek.daggers.async.connector;

import com.gojek.daggers.postprocessor.parser.HttpExternalSourceConfig;
import com.gojek.daggers.postprocessor.parser.OutputMapping;
import com.gojek.daggers.utils.stats.Aspects;
import com.gojek.daggers.utils.stats.StatsManager;
import com.gojek.esb.aggregate.surge.SurgeFactorLogMessage;
import com.gojek.esb.booking.BookingLogMessage;
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

    @Mock
    private HttpExternalSourceConfig httpExternalSourceConfig;

    @Mock
    private OutputMapping outputMapping1;

    @Mock
    private OutputMapping outputMapping2;

    private Row outputRow;
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
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(false);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(404);
        httpResponseHandler.onCompleted(response);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputRow));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_4XX);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIs5XX() throws Exception {
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(false);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(502);
        httpResponseHandler.onCompleted(response);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputRow));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_5XX);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIsOtherThan5XXAnd4XX() throws Exception {
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(false);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(302);
        httpResponseHandler.onCompleted(response);
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_STATUS);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputRow));
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIs4XX() throws Exception {
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(true);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(404);
        httpResponseHandler.onCompleted(response);
        verify(resultFuture).completeExceptionally(any(RuntimeException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_4XX);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIs5XX() throws Exception {
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(true);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(502);
        httpResponseHandler.onCompleted(response);
        verify(resultFuture).completeExceptionally(any(RuntimeException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_5XX);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIsOtherThan5XXAnd4XX() throws Exception {
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(true);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(302);
        httpResponseHandler.onCompleted(response);
        verify(resultFuture).completeExceptionally(any(RuntimeException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_STATUS);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndOnThrowable() throws Exception {
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(false);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        Throwable throwable = new Throwable("throwable message");
        httpResponseHandler.onThrowable(throwable);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputRow));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_ERRORS);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndOnThrowable() throws Exception {
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(true);
        outputRow.setField(0, "test");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        Throwable throwable = new Throwable("throwable message");
        httpResponseHandler.onThrowable(throwable);
        verify(resultFuture).completeExceptionally(any(RuntimeException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_ERRORS);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPopulateSingleResultFromHttpCallInInputRow() throws Exception {
        when(httpExternalSourceConfig.getEndpoint()).thenReturn("http://localhost");
        when(httpExternalSourceConfig.getBodyField()).thenReturn("request_body");
        HashMap<String, OutputMapping> outputMappings = new HashMap<>();
        outputMappings.put("surge_factor", outputMapping1);
        when(outputMapping1.getPath()).thenReturn("$.surge");
        when(httpExternalSourceConfig.getOutputMapping()).thenReturn(outputMappings);

        Row inputRow = new Row(2);
        inputRow.setField(0, "body");
        Row resultRow = new Row(2);
        resultRow.setField(0, "body");
        resultRow.setField(1, 0.732f);
        columnNames = new String[]{"request_body", "surge_factor"};

        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(inputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732\n" +
                "}");
        httpResponseHandler.onCompleted(response);
        verify(statsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultRow));
    }

    @Test
    public void shouldPopulateMultipleResultsFromHttpCallInInputRow() throws Exception {
        when(httpExternalSourceConfig.getEndpoint()).thenReturn("http://localhost");
        when(httpExternalSourceConfig.getBodyField()).thenReturn("request_body");
        HashMap<String, OutputMapping> outputMappings = new HashMap<>();
        outputMappings.put("surge_factor", outputMapping1);
        when(outputMapping1.getPath()).thenReturn("$.surge");
        when(httpExternalSourceConfig.getOutputMapping()).thenReturn(outputMappings);
        when(outputMapping2.getPath()).thenReturn("$.prediction");
        outputMappings.put("s2_id_level", outputMapping2);

        Row inputRow = new Row(3);
        inputRow.setField(0, "body");
        Row resultRow = new Row(3);
        resultRow.setField(0, "body");
        resultRow.setField(1, 0.732f);
        resultRow.setField(2, 345);
        columnNames = new String[]{"request_body", "surge_factor", "s2_id_level"};

        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(inputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732,\n" +
                "  \"prediction\": 345\n" +
                "}");
        httpResponseHandler.onCompleted(response);
        verify(statsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(resultFuture, times(1)).complete(Collections.singleton(resultRow));
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowExcpetionIfFieldNotFoundInFieldDescriptor() throws Exception {
        descriptor = BookingLogMessage.getDescriptor();

        when(httpExternalSourceConfig.getEndpoint()).thenReturn("http://localhost");
        when(httpExternalSourceConfig.getBodyField()).thenReturn("request_body");
        HashMap<String, OutputMapping> outputMappings = new HashMap<>();
        outputMappings.put("surge_factor", outputMapping1);
        when(outputMapping1.getPath()).thenReturn("$.surge");
        when(httpExternalSourceConfig.getOutputMapping()).thenReturn(outputMappings);

        Row inputRow = new Row(2);
        inputRow.setField(0, "body");
        Row resultRow = new Row(2);
        resultRow.setField(0, "body");
        resultRow.setField(1, 0.732f);
        columnNames = new String[]{"request_body", "surge_factor"};

        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(inputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732\n" +
                "}");
        try {
            httpResponseHandler.onCompleted(response);
        } catch (Exception e) {

        }
        verify(resultFuture, times(1)).completeExceptionally(any(IllegalArgumentException.class));
    }

    @Test
    public void shouldThrowExceptionIfPathIsWrongIfFailOnErrorsTrue() throws Exception {
        when(httpExternalSourceConfig.getEndpoint()).thenReturn("http://localhost");
        when(httpExternalSourceConfig.getBodyField()).thenReturn("request_body");
        HashMap<String, OutputMapping> outputMappings = new HashMap<>();
        outputMappings.put("surge_factor", outputMapping1);
        when(outputMapping1.getPath()).thenReturn("$.wrong_path");
        when(httpExternalSourceConfig.getOutputMapping()).thenReturn(outputMappings);

        Row inputRow = new Row(2);
        inputRow.setField(0, "body");
        Row resultRow = new Row(2);
        resultRow.setField(0, "body");
        resultRow.setField(1, 0.732f);
        columnNames = new String[]{"request_body", "surge_factor"};

        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(inputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        httpResponseHandler.start();
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732\n" +
                "}");
        httpResponseHandler.onCompleted(response);
        verify(resultFuture, times(1)).completeExceptionally(any(RuntimeException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_READING_PATH);
    }
}
