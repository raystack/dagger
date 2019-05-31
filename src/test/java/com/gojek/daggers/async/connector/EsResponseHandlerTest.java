package com.gojek.daggers.async.connector;

import com.gojek.daggers.utils.stats.StatsManager;
import com.gojek.daggers.utils.RowMaker;
import com.gojek.esb.driverprofile.DriverProfileLogMessage;
import com.google.protobuf.Descriptors;
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
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.*;

public class EsResponseHandlerTest {

    private ResultFuture resultFuture;
    private Descriptors.Descriptor descriptor;
    private StatsManager statsManager;
    private Row input;
    private Row value;
    private EsResponseHandler esResponseHandler;
    private Response response;

    @Before
    public void setUp() throws Exception {
        input = new Row(1);
        value = mock(Row.class);
        input.setField(0, value);
        when(value.getField(5)).thenReturn(null);
        resultFuture = mock(ResultFuture.class);
        descriptor = DriverProfileLogMessage.getDescriptor();
        statsManager = mock(StatsManager.class);
        esResponseHandler = new EsResponseHandler(input, resultFuture, descriptor, 0, statsManager);
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

        esResponseHandler.start();
        esResponseHandler.onSuccess(response);
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
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

        esResponseHandler.start();
        esResponseHandler.onSuccess(response);
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
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

        esResponseHandler.start();
        esResponseHandler.onSuccess(response);
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
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

        esResponseHandler.start();
        esResponseHandler.onSuccess(response);
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
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

        MockUp<RowMaker> mockUpRowMaker = new MockUp<RowMaker>() {
            @Mock
            public Row makeRow(Map<String, Object> inputMap, Descriptors.Descriptor descriptor) throws IOException {
                throw new IOException("RowMaker failed");
            }
        };

        esResponseHandler.start();
        esResponseHandler.onSuccess(response);
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
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
        esResponseHandler.start();
        esResponseHandler.onFailure(new ResponseException(response));
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
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
        esResponseHandler.start();
        esResponseHandler.onFailure(new ResponseException(response));
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
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
        esResponseHandler.start();
        esResponseHandler.onFailure(new ResponseException(response));
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
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
        esResponseHandler.start();
        esResponseHandler.onFailure(new IOException(""));
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
    }

}