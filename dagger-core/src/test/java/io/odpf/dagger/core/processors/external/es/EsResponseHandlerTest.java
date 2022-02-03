package io.odpf.dagger.core.processors.external.es;

import com.google.protobuf.Descriptors;
import com.jayway.jsonpath.PathNotFoundException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestEnrichedBookingLogMessage;
import io.odpf.dagger.consumer.TestProfile;
import io.odpf.dagger.core.exception.HttpFailureException;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.OutputMapping;
import io.odpf.dagger.core.processors.common.PostResponseTelemetry;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.common.serde.proto.protohandler.ProtoHandlerFactory;
import io.odpf.dagger.common.serde.proto.protohandler.RowFactory;
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
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class EsResponseHandlerTest {

    @org.mockito.Mock
    private ErrorReporter errorReporter;

    private ResultFuture resultFuture;
    private Descriptors.Descriptor defaultDescriptor;
    private MeterStatsManager meterStatsManager;
    private EsResponseHandler esResponseHandler;
    private Response defaultResponse;
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
        esSourceConfig = getEsSourceConfigBuilder().createEsSourceConfig();
        resultFuture = mock(ResultFuture.class);
        defaultDescriptor = TestEnrichedBookingLogMessage.getDescriptor();
        meterStatsManager = mock(MeterStatsManager.class);
        inputColumnNames = new String[3];
        outputColumnNames = new ArrayList<>();
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);

        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, defaultDescriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        defaultResponse = mock(Response.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(defaultResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        HttpEntity httpEntity = mock(HttpEntity.class);
        when(defaultResponse.getEntity()).thenReturn(httpEntity);
    }

    private EsSourceConfigBuilder getEsSourceConfigBuilder() {
        return new EsSourceConfigBuilder()
                .setHost("localhost")
                .setPort("9200")
                .setUser("")
                .setPassword("")
                .setEndpointPattern("")
                .setEndpointVariables("driver_id")
                .setType("test")
                .setCapacity("30")
                .setConnectTimeout("5000")
                .setRetryTimeout("5000")
                .setSocketTimeout("5000")
                .setStreamTimeout("5000")
                .setFailOnErrors(false)
                .setOutputMapping(outputMapping)
                .setMetricId("metricId_01")
                .setRetainResponseType(false);
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
        esSourceConfig = getEsSourceConfigBuilder().createEsSourceConfig();
        outputColumnNames.add("driver_profile");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, defaultDescriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        HashMap<String, Object> outputDataMap = new HashMap<>();
        outputDataMap.put("driver_id", 12345);
        outputData.setField(0, RowFactory.createRow(outputDataMap, TestProfile.getDescriptor()));
        outputStreamData.setField(1, outputData);


        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(defaultResponse);

        System.out.println("WOH " + outputStreamData);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));

        mockUp.tearDown();
    }

    @Test
    public void shouldCompleteResultFutureWithInputAsObjectIfTypeIsNotPassedAndRetainResponseTypeIsTrue() {
        MockUp<EntityUtils> mockUp = new MockUp<EntityUtils>() {
            @Mock
            public String toString(HttpEntity entity) {
                return "{\"_source\": {\"driver_id\":\"12345\"}}";
            }
        };

        outputMapping.put("driver_profile", new OutputMapping("$._source"));
        esSourceConfig = getEsSourceConfigBuilder()
                .setType(null)
                .setRetainResponseType(true)
                .createEsSourceConfig();
        outputColumnNames.add("driver_profile");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, defaultDescriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        HashMap<String, Object> outputDataMap = new HashMap<>();
        outputDataMap.put("driver_id", "12345");
        outputData.setField(0, outputDataMap);
        outputStreamData.setField(1, outputData);


        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(defaultResponse);

        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamData));

        mockUp.tearDown();
    }

    @Test
    public void shouldNotPopulateResultAsObjectIfTypeIsNotPassedAndRetainResponseTypeIsFalse() {
        MockUp<EntityUtils> mockUp = new MockUp<EntityUtils>() {
            @Mock
            public String toString(HttpEntity entity) {
                return "{\"_source\": {\"driver_id\":\"12345\"}}";
            }
        };

        outputMapping.put("driver_profile", new OutputMapping("$._source"));
        esSourceConfig = getEsSourceConfigBuilder()
                .setType(null)
                .setRetainResponseType(false)
                .createEsSourceConfig();
        outputColumnNames.add("driver_profile");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, defaultDescriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        HashMap<String, Object> outputDataMap = new HashMap<>();
        outputDataMap.put("driver_id", 12345);
        outputData.setField(0, RowFactory.createRow(outputDataMap, TestProfile.getDescriptor()));
        outputStreamData.setField(1, outputData);


        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(defaultResponse);

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

        defaultDescriptor = TestBookingLogMessage.getDescriptor();
        outputMapping.put("driver_id", new OutputMapping("$._source.driver_id"));
        esSourceConfig = getEsSourceConfigBuilder().createEsSourceConfig();
        outputColumnNames.add("driver_id");
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, defaultDescriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        outputData.setField(0, ProtoHandlerFactory.getProtoHandler(defaultDescriptor.findFieldByName("driver_id")).transformFromPostProcessor(12345));
        outputStreamData.setField(1, outputData);

        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(defaultResponse);

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
        esSourceConfig = getEsSourceConfigBuilder().createEsSourceConfig();

        esResponseHandler.startTimer();
        esResponseHandler.onSuccess(defaultResponse);

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
        esResponseHandler.onSuccess(defaultResponse);

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
        esResponseHandler.onSuccess(defaultResponse);

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
        esResponseHandler.onSuccess(defaultResponse);

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
        esResponseHandler.onSuccess(defaultResponse);

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

        esSourceConfig = getEsSourceConfigBuilder()
                .setFailOnErrors(true)
                .createEsSourceConfig();

        esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, defaultDescriptor, resultFuture, errorReporter, new PostResponseTelemetry());

        esResponseHandler.startTimer();
        esResponseHandler.onFailure(new IOException("hello-world"));
        ArgumentCaptor<HttpFailureException> httpFailureExceptionArgumentCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(resultFuture, times(1)).completeExceptionally(httpFailureExceptionArgumentCaptor.capture());
        assertEquals("EsResponseHandler : Failed with error. hello-world", httpFailureExceptionArgumentCaptor.getValue().getMessage());

        ArgumentCaptor<HttpFailureException> reportFailureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1)).reportFatalException(reportFailureCaptor.capture());
        assertEquals("EsResponseHandler : Failed with error. hello-world", reportFailureCaptor.getValue().getMessage());
    }
}
