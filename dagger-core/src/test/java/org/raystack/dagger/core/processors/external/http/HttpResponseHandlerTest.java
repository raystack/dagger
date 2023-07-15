package org.raystack.dagger.core.processors.external.http;

import com.google.protobuf.Descriptors;
import org.raystack.dagger.common.metrics.aspects.Aspects;
import org.raystack.dagger.common.metrics.managers.MeterStatsManager;
import org.raystack.dagger.core.exception.HttpFailureException;
import org.raystack.dagger.core.metrics.aspects.ExternalSourceAspects;
import org.raystack.dagger.core.metrics.reporters.ErrorReporter;
import org.raystack.dagger.core.processors.ColumnNameManager;
import org.raystack.dagger.core.processors.common.OutputMapping;
import org.raystack.dagger.core.processors.common.PostResponseTelemetry;
import org.raystack.dagger.core.processors.common.RowManager;
import org.raystack.dagger.consumer.TestBookingLogMessage;
import org.raystack.dagger.consumer.TestSurgeFactorLogMessage;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.Response;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpResponseHandlerTest {

    @Mock
    private ResultFuture<Row> resultFuture;

    @Mock
    private Response response;

    @Mock
    private MeterStatsManager meterStatsManager;

    @Mock
    private HttpSourceConfig httpSourceConfig;

    @Mock
    private ErrorReporter errorReporter;

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
        descriptor = TestSurgeFactorLogMessage.getDescriptor();
        outputColumnNames = Collections.singletonList("value");
        inputColumnNames = new String[] {"order_id", "customer_id", "driver_id"};
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
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIs404() {
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(response.getStatusCode()).thenReturn(404);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.FAILURE_CODE_404);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        ArgumentCaptor<HttpFailureException> failureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1)).reportNonFatalException(failureCaptor.capture());
        assertEquals("Received status code : 404", failureCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIs4XXOtherThan404() {
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(response.getStatusCode()).thenReturn(402);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.FAILURE_CODE_4XX);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        ArgumentCaptor<HttpFailureException> failureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1)).reportNonFatalException(failureCaptor.capture());
        assertEquals("Received status code : 402", failureCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIs5XX() {
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(response.getStatusCode()).thenReturn(502);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.FAILURE_CODE_5XX);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);

        ArgumentCaptor<HttpFailureException> failureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1)).reportNonFatalException(failureCaptor.capture());
        assertEquals("Received status code : 502", failureCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndStatusCodeIsOtherThan5XXAnd4XX() {
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(response.getStatusCode()).thenReturn(302);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.OTHER_ERRORS);
        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        ArgumentCaptor<HttpFailureException> failureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1)).reportNonFatalException(failureCaptor.capture());
        assertEquals("Received status code : 302", failureCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIs404() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(response.getStatusCode()).thenReturn(404);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture).completeExceptionally(any(HttpFailureException.class));
        ArgumentCaptor<HttpFailureException> argumentCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1))
                .reportFatalException(argumentCaptor.capture());
        assertEquals("Received status code : 404", argumentCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.FAILURE_CODE_404);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIs4XXOtherThan404() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(response.getStatusCode()).thenReturn(400);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        ArgumentCaptor<HttpFailureException> failureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(resultFuture, times(1)).completeExceptionally(failureCaptor.capture());
        assertEquals("Received status code : 400", failureCaptor.getValue().getMessage());
        verify(errorReporter, times(1)).reportFatalException(any(HttpFailureException.class));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.FAILURE_CODE_4XX);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIs5XX() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(response.getStatusCode()).thenReturn(502);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        ArgumentCaptor<HttpFailureException> failureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(resultFuture, times(1)).completeExceptionally(failureCaptor.capture());
        assertEquals("Received status code : 502", failureCaptor.getValue().getMessage());
        verify(errorReporter, times(1)).reportFatalException(any(HttpFailureException.class));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.FAILURE_CODE_5XX);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndStatusCodeIsOtherThan5XXAnd4XX() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(response.getStatusCode()).thenReturn(302);

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        ArgumentCaptor<HttpFailureException> failureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(resultFuture, times(1)).completeExceptionally(failureCaptor.capture());
        assertEquals("Received status code : 302", failureCaptor.getValue().getMessage());
        verify(errorReporter, times(1)).reportFatalException(any(HttpFailureException.class));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.OTHER_ERRORS);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPassInputIfFailOnErrorFalseAndOnThrowable() {
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        Throwable throwable = new Throwable("throwable message");

        httpResponseHandler.startTimer();
        httpResponseHandler.onThrowable(throwable);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        ArgumentCaptor<HttpFailureException> failureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1)).reportNonFatalException(failureCaptor.capture());
        assertEquals("throwable message", failureCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowErrorIfFailOnErrorTrueAndOnThrowable() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        Throwable throwable = new Throwable("throwable message");

        httpResponseHandler.startTimer();
        httpResponseHandler.onThrowable(throwable);

        verify(resultFuture).completeExceptionally(any(RuntimeException.class));
        ArgumentCaptor<HttpFailureException> failureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1)).reportFatalException(failureCaptor.capture());
        assertEquals("throwable message", failureCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.OTHER_ERRORS);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPopulateSingleResultFromHttpCallInInputRow() {
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputColumnNames = Collections.singletonList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732f);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n"
                + "  \"surge\": 0.732\n"
                + "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }

    @Test
    public void shouldPopulateMultipleResultsFromHttpCallInInputRow() {
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputMapping.put("s2_id_level", new OutputMapping("$.prediction"));
        outputColumnNames = Arrays.asList("surge_factor", "s2_id_level");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732f);
        outputData.setField(1, 345);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n"
                + "  \"surge\": 0.732,\n"
                + "  \"prediction\": 345\n"
                + "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.SUCCESS_RESPONSE);
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldThrowExceptionIfFieldNotFoundInFieldDescriptorWhenTypeIsPassed() {
        httpConfigType = "org.raystack.dagger.consumer.TestBookingLogMessage";
        descriptor = TestBookingLogMessage.getDescriptor();
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputColumnNames = Collections.singletonList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n"
                + "  \"surge\": 0.732\n"
                + "}");
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());

        httpResponseHandler.startTimer();
        assertThrows(NullPointerException.class,
                () -> httpResponseHandler.onCompleted(response));
        verify(resultFuture, times(1)).completeExceptionally(any(IllegalArgumentException.class));
    }

    @Test
    public void shouldThrowExceptionIfPathIsWrongIfFailOnErrorsTrue() {
        outputMapping.put("surge_factor", new OutputMapping("invalidPath"));
        outputColumnNames = Collections.singletonList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732f);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n"
                + "  \"surge\": 0.732\n"
                + "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(resultFuture, times(1)).completeExceptionally(any(RuntimeException.class));
        verify(errorReporter, times(1)).reportFatalException(any(RuntimeException.class));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.FAILURES_ON_READING_PATH);
    }

    @Test
    public void shouldPopulateResultAsObjectIfTypeIsNotPassedAndRetainResponseTypeIsTrue() {
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputColumnNames = Collections.singletonList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, null, "345", headers, outputMapping, "metricId_02", true);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n"
                + "  \"surge\": 0.732\n"
                + "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }

    @Test
    public void shouldNotPopulateResultAsObjectIfTypeIsNotPassedAndRetainResponseTypeIsFalse() {
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputColumnNames = Collections.singletonList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, null, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732f);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\n"
                + "  \"surge\": 0.732\n"
                + "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }

    @Test
    public void shouldHandleAnySuccessResponseCodeOtherThan200() {
        outputMapping.put("surge_factor", new OutputMapping("$.surge"));
        outputColumnNames = Collections.singletonList("surge_factor");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        Row resultStreamData = new Row(2);
        Row outputData = new Row(2);
        outputData.setField(0, 0.732f);
        resultStreamData.setField(0, inputData);
        resultStreamData.setField(1, outputData);
        when(response.getStatusCode()).thenReturn(201);
        when(response.getResponseBody()).thenReturn("{\n"
                + "  \"surge\": 0.732\n"
                + "}");

        httpResponseHandler.startTimer();
        httpResponseHandler.onCompleted(response);

        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(resultStreamData));
    }
}
