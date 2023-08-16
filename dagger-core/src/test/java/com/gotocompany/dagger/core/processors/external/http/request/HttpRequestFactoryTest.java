package com.gotocompany.dagger.core.processors.external.http.request;

import com.gotocompany.dagger.core.exception.InvalidHttpVerbException;
import com.gotocompany.dagger.core.processors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpRequestFactoryTest {

    @Mock
    private AsyncHttpClient httpClient;

    @Mock
    private BoundRequestBuilder request;

    private HttpSourceConfig httpSourceConfig;
    private ArrayList<Object> requestVariablesValues;
    private ArrayList<Object> headerVariablesValues;
    private ArrayList<Object> endpointVariablesValues;
    private boolean retainResponseType;

    @Before
    public void setup() {
        initMocks(this);
        requestVariablesValues = new ArrayList<>();
        headerVariablesValues = new ArrayList<>();
        requestVariablesValues.add(1);
        endpointVariablesValues = new ArrayList<>();
        retainResponseType = false;
    }

    @Test
    public void shouldReturnPostRequestOnTheBasisOfConfiguration() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", null, "POST", "{\"key\": \"%s\"}", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", retainResponseType);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(request);
        when(request.setBody("{\"key\": \"123456\"}")).thenReturn(request);
        HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());

        verify(httpClient, times(1)).preparePost("http://localhost:8080/test");
        verify(httpClient, times(0)).prepareGet(any(String.class));
        verify(httpClient, times(0)).preparePut(any(String.class));
    }

    @Test
    public void shouldReturnPostRequestWithMultiEndpointVariablesOnTheBasisOfConfiguration() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test/%s/%s", "exp, 222", "POST", "{\"key\": \"%s\"}", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", retainResponseType);
        when(httpClient.preparePost("http://localhost:8080/test/exp/222")).thenReturn(request);
        when(request.setBody("{\"key\": \"123456\"}")).thenReturn(request);
        endpointVariablesValues.add("exp");
        endpointVariablesValues.add("222");
        HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());

        verify(httpClient, times(1)).preparePost("http://localhost:8080/test/exp/222");
        verify(httpClient, times(0)).prepareGet(any(String.class));
        verify(httpClient, times(0)).preparePut(any(String.class));
    }

    @Test
    public void shouldReturnGetRequestOnTheBasisOfConfiguration() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", null, "GET", "/key/%s", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", retainResponseType);
        when(httpClient.prepareGet("http://localhost:8080/test/key/1")).thenReturn(request);
        HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());

        verify(httpClient, times(1)).prepareGet("http://localhost:8080/test/key/1");
        verify(httpClient, times(0)).preparePost(any(String.class));
        verify(httpClient, times(0)).preparePut(any(String.class));
    }

    @Test
    public void shouldReturnGetRequestWithMultiEndpointVariablesOnTheBasisOfConfiguration() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test/%s/%s", "123, 332", "GET", "/key/%s", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", retainResponseType);
        when(httpClient.prepareGet("http://localhost:8080/test/key/1")).thenReturn(request);
        endpointVariablesValues.add("123");
        endpointVariablesValues.add("332");
        HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());
        verify(httpClient, times(1)).prepareGet("http://localhost:8080/test/123/332/key/1");
        verify(httpClient, times(0)).preparePost(any(String.class));
        verify(httpClient, times(0)).preparePut(any(String.class));
    }

    @Test
    public void shouldReturnPutRequestOnTheBasisOfConfiguration() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test/%s", "123", "PUT", "{\"key\": \"%s\"}", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", retainResponseType);
        when(httpClient.preparePut("http://localhost:8080/test/123")).thenReturn(request);
        when(request.setBody("{\"key\": \"123456\"}")).thenReturn(request);
        endpointVariablesValues.add("123");
        HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());

        verify(httpClient, times(1)).preparePut("http://localhost:8080/test/123");
        verify(httpClient, times(0)).prepareGet(any(String.class));
        verify(httpClient, times(0)).preparePost(any(String.class));
    }

    @Test
    public void shouldReturnPutRequestWithMultiEndpointVariablesOnTheBasisOfConfiguration() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test/%s/abc/%s", "123, 321, asd", "PUT", "{\"key\": \"%s\"}", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", retainResponseType);
        when(httpClient.preparePut("http://localhost:8080/test/123/abc/asd")).thenReturn(request);
        when(request.setBody("{\"key\": \"123456\"}")).thenReturn(request);
        endpointVariablesValues.add("123");
        endpointVariablesValues.add("asd");
        HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());

        verify(httpClient, times(1)).preparePut("http://localhost:8080/test/123/abc/asd");
        verify(httpClient, times(0)).prepareGet(any(String.class));
        verify(httpClient, times(0)).preparePost(any(String.class));
    }


    @Test
    public void shouldThrowExceptionForUnsupportedHttpVerb() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "PATCH", "/key/%s", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", retainResponseType);
        assertThrows(InvalidHttpVerbException.class, () -> HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray()));
    }

}
