package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.exception.HttpFailureException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.PostResponseTelemetry;
import com.gojek.daggers.protoHandler.ProtoHandlerFactory;
import com.gojek.daggers.protoHandler.RowFactory;
import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.esb.fraud.DriverProfileFlattenLogMessage;
import com.gojek.esb.fraud.EnrichedBookingLogMessage;
import com.google.protobuf.Descriptors;
import com.jayway.jsonpath.PathNotFoundException;
import mockit.Mock;
import mockit.MockUp;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.http.*;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class EsResponseHandlerTest {

    @org.mockito.Mock
    private ErrorReporter errorReporter;

    private ResultFuture resultFuture;
    private Descriptors.Descriptor descriptor;
    private MeterStatsManager meterStatsManager;
    private EsResponseHandler esResponseHandler;
    private Response response;
    private EsSourceConfig esSourceConfig;
    private RowManager rowManager;
    private ColumnNameManager columnNameManager;
    private String[] inputColumnNames;
    private ArrayList<String> outputColumnNames;
    private HashMap<String, OutputMapping> outputMapping;
    private Row outputStreamData;
    private Row outputData;

    @Before
    public void setUp() {
        initMocks(this);
        Row inputStreamData = new Row(2);
        inputStreamData.setField(0, new Row(3));
        inputStreamData.setField(1, new Row(4));
        outputStreamData = new Row(2);
        outputStreamData.setField(0, new Row(3));
        outputData = new Row(4);
        outputStreamData.setField(1, outputData);
        rowManager = new RowManager(inputStreamData);
        outputMapping = new HashMap<>();
        esSourceConfig = new EsSourceConfig("localhost", "9200", "",
                "driver_id", "com.gojek.esb.fraud.EnrichedBookingLogMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01");
        resultFuture = mock(ResultFuture.class);
        descriptor = EnrichedBookingLogMessage.getDescriptor();
        meterStatsManager = mock(MeterStatsManager.class);
        inputColumnNames = new String[3];
        outputColumnNames = new ArrayList<>();
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);

        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        response = mock(Response.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        HttpEntity httpEntity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(httpEntity);
    }

    @Test
    public void shouldCompleteResultFutureWithInput() {
        MockUp<EntityUtils> mockUp = new MockUp<EntityUtils>() {
            @Mock
            public String toString(HttpEntity entity) {
                return "{\"_source\": {\"driver_id\":\"12345\"}}";
            }
        };

        outputMapping.put("driver_profile", new OutputMapping("$._source"));
        esSourceConfig = new EsSourceConfig("localhost", "9200", "",
                "driver_id", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01");
        outputColumnNames.add("driver_profile");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        HashMap<String, Object> outputDataMap = new HashMap<>();
        outputDataMap.put("driver_id", 12345);
        outputData.setField(0, RowFactory.createRow(outputDataMap, DriverProfileFlattenLogMessage.getDescriptor()));
        outputStreamData.setField(1, outputData);


        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));

        mockUp.tearDown();
    }

    @Test
    public void shouldCompleteResultFutureWithInputForPrimitiveData() {
        MockUp<EntityUtils> mockUp = new MockUp<EntityUtils>() {
            @Mock
            public String toString(HttpEntity entity) {
                return "{\"_source\": {\"driver_id\":\"12345\"}}";
            }
        };

        descriptor = DriverProfileFlattenLogMessage.getDescriptor();
        outputMapping.put("driver_id", new OutputMapping("$._source.driver_id"));
        esSourceConfig = new EsSourceConfig("localhost", "9200", "",
                "driver_id", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01");
        outputColumnNames.add("driver_id");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        outputData.setField(0, ProtoHandlerFactory.getProtoHandler(descriptor.findFieldByName("driver_id")).transformFromPostProcessor(12345));
        outputStreamData.setField(1, outputData);

        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));

        mockUp.tearDown();
    }

    @Test
    public void shouldCompleteResultFutureExceptionallyWhenPathDoesNotExists() {
        MockUp<EntityUtils> mockUp = new MockUp<EntityUtils>() {
            @Mock
            public String toString(HttpEntity entity) {
                return "{\"_source\": {\"driver_id\":\"12345\"}}";
            }
        };
        outputMapping.put("driver_id", new OutputMapping("$.invalidPath"));
        esSourceConfig = new EsSourceConfig("localhost", "9200", "",
                "driver_id", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01");

        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(response);

        verify(resultFuture, times(1)).completeExceptionally(any(PathNotFoundException.class));

        mockUp.tearDown();
    }

    @Test
    public void shouldHandleParseExceptionAndReturnInput() {
        MockUp<EntityUtils> mockUp = new MockUp<EntityUtils>() {
            @Mock
            public String toString(HttpEntity entity) {
                throw new ParseException("Parsing failed!!!!");
            }
        };

        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));

        mockUp.tearDown();
    }

    @Test
    public void shouldHandleIOExceptionAndReturnInput() {
        MockUp<EntityUtils> mockUp = new MockUp<EntityUtils>() {
            @Mock
            public String toString(HttpEntity entity) throws IOException {
                throw new IOException("IO failed!!!!");
            }
        };

        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));

        mockUp.tearDown();
    }

    @Test
    public void shouldHandleExceptionAndReturnInput() {
        MockUp<EntityUtils> mockUp = new MockUp<EntityUtils>() {
            @Mock
            public String toString(HttpEntity entity) throws IOException {
                throw new NullPointerException("Null!!!");
            }
        };

        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));

        mockUp.tearDown();
    }

    @Test
    public void shouldHandleResponseParsingIOExceptionAndReturnInput() {
        MockUp<EntityUtils> mockUpEntityUtils = new MockUp<EntityUtils>() {
            @Mock
            public String toString(HttpEntity entity) {
                return "{\"_source\": {\"driver_id\":\"12345\"}}";
            }
        };
        MockUp<RowFactory> mockUpRowMaker = new MockUp<RowFactory>() {
            @Mock
            public Row createRow(Map<String, Object> inputMap, Descriptors.Descriptor descriptor) throws IOException {
                throw new IOException("RowMaker failed");
            }
        };

        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(response);

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));

        mockUpEntityUtils.tearDown();
        mockUpRowMaker.tearDown();
    }

    @Test
    public void shouldHandleOnFailure() throws IOException {
        Response response = mock(Response.class);
        RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn("GET");
        when(response.getRequestLine()).thenReturn(requestLine);
        HttpHost httpHost = new HttpHost("test", 9091, "test");
        when(response.getHost()).thenReturn(httpHost);
        when(requestLine.getUri()).thenReturn("/drivers/driver/11223344545");
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.toString()).thenReturn("Test");
        when(response.getStatusLine()).thenReturn(statusLine);

        esResponseHandler.startTimer();
        esResponseHandler.onFailure(new ResponseException(response));

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));
    }

    @Test
    public void shouldHandleForNotFoundOnFailure() throws IOException {
        Response response = mock(Response.class);
        RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn("GET");
        when(response.getRequestLine()).thenReturn(requestLine);
        HttpHost httpHost = new HttpHost("test", 9091, "test");
        when(response.getHost()).thenReturn(httpHost);
        when(requestLine.getUri()).thenReturn("/drivers/driver/11223344545");
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.toString()).thenReturn("Test");
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(404);

        esResponseHandler.startTimer();
        esResponseHandler.onFailure(new ResponseException(response));

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));
    }

    @Test
    public void shouldHandleForRetryStatusOnFailure() throws IOException {
        Response response = mock(Response.class);
        RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn("GET");
        when(response.getRequestLine()).thenReturn(requestLine);
        HttpHost httpHost = new HttpHost("test", 9091, "test");
        when(response.getHost()).thenReturn(httpHost);
        when(requestLine.getUri()).thenReturn("/drivers/driver/11223344545");
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.toString()).thenReturn("Test");
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(502);


        esResponseHandler.startTimer();
        esResponseHandler.onFailure(new ResponseException(response));

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));
    }

    @Test
    public void shouldHandleForNonResponseExceptionOnFailure() throws IOException {
        Response response = mock(Response.class);
        RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn("GET");
        when(response.getRequestLine()).thenReturn(requestLine);
        HttpHost httpHost = new HttpHost("test", 9091, "test");
        when(response.getHost()).thenReturn(httpHost);
        when(requestLine.getUri()).thenReturn("/drivers/driver/11223344545");
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.toString()).thenReturn("Test");
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(502);

        esResponseHandler.startTimer();
        esResponseHandler.onFailure(new IOException(""));

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));
    }

    @Test
    public void shouldExceptionWhenFailOnErrorTrue() throws IOException {
        Response response = mock(Response.class);
        RequestLine requestLine = mock(RequestLine.class);
        when(requestLine.getMethod()).thenReturn("GET");
        when(response.getRequestLine()).thenReturn(requestLine);
        HttpHost httpHost = new HttpHost("test", 9091, "test");
        when(response.getHost()).thenReturn(httpHost);
        when(requestLine.getUri()).thenReturn("/drivers/driver/11223344545");
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.toString()).thenReturn("Test");
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(502);

        esSourceConfig = new EsSourceConfig("localhost", "9200", "",
                "driver_id", "com.gojek.esb.fraud.EnrichedBookingLogMessage", "30",
                "5000", "5000", "5000", "5000", true, outputMapping, "metricId_01");

        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());

        esResponseHandler.startTimer();
        esResponseHandler.onFailure(new IOException(""));

        verify(resultFuture, times(1)).completeExceptionally(any(HttpFailureException.class));
        verify(errorReporter, times(1)).reportFatalException(any(HttpFailureException.class));
    }
}