package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.exception.HttpFailureException;
import com.gojek.daggers.metrics.Aspects;
import com.gojek.daggers.metrics.StatsManager;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.common.RowManager;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.gojek.daggers.metrics.ExternalSourceAspects.*;
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
    private HttpSourceConfig httpSourceConfig;

    private Descriptors.Descriptor descriptor;
    private List<String> outputColumnNames;
    private String[] inputColumnNames;
    private HashMap<String, OutputMapping> outputMapping;
    private HashMap<String, String> headers;
    private String httpConfigType;
    private Row streamData;
    private RowManager rowManager;
    private ColumnNameManager columnNameManager;
    private Row inputData;

    @Before
    public void setup() {
        initMocks(this);
        descriptor = SurgeFactorLogMessage.getDescriptor();
        outputColumnNames = Arrays.asList("value");
        inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        outputMapping = new HashMap<>();
        headers = new HashMap<>();
        headers.put("content-type", "application/json");
        httpConfigType = "test";
        streamData = new Row(2);
        inputData = new Row(3);
        inputData.setField(1, "123456");
        streamData.setField(0, inputData);
        streamData.setField(1, new Row(2));
        rowManager = new RowManager(streamData);
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping);
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIs4XX() throws Exception {
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        when(response.getStatusCode()).thenReturn(404);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_4XX);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIs5XX() throws Exception {
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        when(response.getStatusCode()).thenReturn(502);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_5XX);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIsOtherThan5XXAnd4XX() throws Exception {
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        when(response.getStatusCode()).thenReturn(302);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_STATUS);
        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIs4XX() throws Exception {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        when(response.getStatusCode()).thenReturn(404);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture).completeExceptionally(any(HttpFailureException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_4XX);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIs5XX() throws Exception {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        when(response.getStatusCode()).thenReturn(502);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture).completeExceptionally(any(HttpFailureException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_5XX);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIsOtherThan5XXAnd4XX() throws Exception {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        when(response.getStatusCode()).thenReturn(302);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture).completeExceptionally(any(HttpFailureException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_STATUS);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndOnThrowable() throws Exception {
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        Throwable throwable = new Throwable("throwable message");

        httpResponseHandler.startTimer();
        httpResponseHandler.onThrowable(throwable);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_ERRORS);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndOnThrowable() throws Exception {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        Throwable throwable = new Throwable("throwable message");

        httpResponseHandler.startTimer();
        httpResponseHandler.onThrowable(throwable);

        verify(resultFuture).completeExceptionally(any(RuntimeException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_HTTP_CALL_OTHER_ERRORS);
        verify(statsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPopulateSingleResultFromHttpCallInInputRow() throws Exception {
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputColumnNames = Arrays.asList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732f);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732\n" +
                "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(statsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }

    @Test
    public void shouldPopulateMultipleResultsFromHttpCallInInputRow() throws Exception {
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputMapping.put("s2_id_level", new OutputMapping("$.prediction"));
        outputColumnNames = Arrays.asList("surge_factor", "s2_id_level");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732f);
        outputData.setField(1, 345);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732,\n" +
                "  \"prediction\": 345\n" +
                "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(statsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowExceptionIfFieldNotFoundInFieldDescriptorWhenTypeIsPassed() throws Exception {
        httpConfigType = "com.gojek.esb.booking.BookingLogMessage";
        descriptor = BookingLogMessage.getDescriptor();
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputColumnNames = Arrays.asList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732\n" +
                "}");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);

        httpResponseHandler.startTimer();
        try {
            httpResponseHandler.onCompleted(response);
        } catch (Exception ignored) {
        } finally {
            verify(resultFuture, times(1)).completeExceptionally(any(IllegalArgumentException.class));
        }
    }

    @Test
    public void shouldThrowExceptionIfPathIsWrongIfFailOnErrorsTrue() throws Exception {
        outputMapping.put("surge_factor", new OutputMapping("invalidPath"));
        outputColumnNames = Arrays.asList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732f);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732\n" +
                "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture, times(1)).completeExceptionally(any(RuntimeException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_READING_PATH);
}

    @Test
    public void shouldNotThrowExceptionIfPathIsWrongIfFailOnErrorsFalse() throws Exception {
        outputMapping.put("surge_factor", new OutputMapping("invalidPath"));
        outputColumnNames = Arrays.asList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732f);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732\n" +
                "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture, times(1)).completeExceptionally(any(RuntimeException.class));
        verify(statsManager, times(1)).markEvent(FAILURES_ON_READING_PATH);
}

    @Test
    public void shouldPopulateResultAsObjectIfTypeIsNotPassed() throws Exception {
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputColumnNames = Arrays.asList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, null, "345", headers, outputMapping);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, descriptor, resultFuture);
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n" +
                "  \"surge\": 0.732\n" +
                "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(statsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(statsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }
}
