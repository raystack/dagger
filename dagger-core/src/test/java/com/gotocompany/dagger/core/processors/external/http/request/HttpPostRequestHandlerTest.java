package com.gotocompany.dagger.core.processors.external.http.request;

import com.gotocompany.dagger.core.exception.InvalidConfigurationException;
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

public class HttpPostRequestHandlerTest {

    @Mock
    private AsyncHttpClient httpClient;

    @Mock
    private BoundRequestBuilder request;

    private HttpSourceConfig httpSourceConfig;
    private ArrayList<Object> requestVariablesValues;
    private ArrayList<Object> dynamicHeaderVariablesValues;

    private ArrayList<Object> endpointVariablesValues;
    @Before
    public void setup() {
        initMocks(this);
        requestVariablesValues = new ArrayList<>();
        requestVariablesValues.add(1);
        dynamicHeaderVariablesValues = new ArrayList<>();
        dynamicHeaderVariablesValues.add("1");
        dynamicHeaderVariablesValues.add("2");
        endpointVariablesValues = new ArrayList<>();
    }

    @Test
    public void shouldReturnTrueForPostVerbOnCanCreate() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", null, "POST", "{\"key\": \"%s\"}", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), dynamicHeaderVariablesValues.toArray(), endpointVariablesValues.toArray());
        assertTrue(httpPostRequestBuilder.canCreate());
    }

    @Test
    public void shouldReturnFalseForVerbOtherThanPostOnCanBuild() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "GET", "{\"key\": \"%s\"}", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), dynamicHeaderVariablesValues.toArray(), endpointVariablesValues.toArray());
        assertFalse(httpPostRequestBuilder.canCreate());
    }

    @Test
    public void shouldBuildPostRequestWithoutHeader() {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(request);
        when(request.setBody("{\"key\": \"1\"}")).thenReturn(request);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "1", "", "", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), dynamicHeaderVariablesValues.toArray(), endpointVariablesValues.toArray());
        assertEquals(request, httpPostRequestBuilder.create());
    }

    @Test
    public void shouldBuildPostRequestWithOnlyDynamicHeader() {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(request);
        when(request.setBody("{\"key\": \"1\"}")).thenReturn(request);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "1", "{\"header_key\": \"%s\"}", "1", "123", "234", false, null, "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), dynamicHeaderVariablesValues.toArray(), endpointVariablesValues.toArray());
        httpPostRequestBuilder.create();
        verify(request, times(1)).addHeader(anyString(), anyString());
        verify(request, times(1)).addHeader("header_key", "1");
    }

    @Test
    public void shouldBuildPostRequestWithDynamicAndStaticHeader() {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(request);
        when(request.setBody("{\"key\": \"1\"}")).thenReturn(request);
        HashMap<String, String> staticHeader = new HashMap<String, String>();
        staticHeader.put("static", "2");
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "1", "{\"header_key\": \"%s\"}", "1", "123", "234", false, null, "type", "345", staticHeader, null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), dynamicHeaderVariablesValues.toArray(), endpointVariablesValues.toArray());
        httpPostRequestBuilder.create();
        verify(request, times(2)).addHeader(anyString(), anyString());
        verify(request, times(1)).addHeader("header_key", "1");
        verify(request, times(1)).addHeader("static", "2");
    }

    @Test
    public void shouldBuildPostRequestWithMultipleDynamicAndStaticHeaders() {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(request);
        when(request.setBody("{\"key\": \"1\"}")).thenReturn(request);
        HashMap<String, String> staticHeader = new HashMap<String, String>();
        staticHeader.put("static", "3");
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "1", "{\"header_key_1\": \"%s\",\"header_key_2\": \"%s\"}", "1,2", "123", "234", false, null, "type", "345", staticHeader, null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), dynamicHeaderVariablesValues.toArray(), endpointVariablesValues.toArray());
        httpPostRequestBuilder.create();
        verify(request, times(3)).addHeader(anyString(), anyString());
        verify(request, times(1)).addHeader("header_key_1", "1");
        verify(request, times(1)).addHeader("header_key_2", "2");
        verify(request, times(1)).addHeader("static", "3");
    }

    @Test
    public void shouldThrowErrorIfHeaderVariablesAreIncompatible() {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(request);
        when(request.setBody("{\"key\": \"1\"}")).thenReturn(request);
        HashMap<String, String> staticHeader = new HashMap<String, String>();
        staticHeader.put("static", "3");
        ArrayList incompatibleHeaderVariablesValues = new ArrayList<>();
        incompatibleHeaderVariablesValues.add("test1");
        incompatibleHeaderVariablesValues.add("test12");
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "1", "{\"header_key_1\": \"%s\",\"header_key_2\": \"%d\"}", "1,2", "123", "234", false, null, "type", "345", staticHeader, null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), incompatibleHeaderVariablesValues.toArray(), endpointVariablesValues.toArray());
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> httpPostRequestBuilder.create());
        assertEquals("pattern config '{\"header_key_1\": \"%s\",\"header_key_2\": \"%d\"}' is incompatible with the variable config '1,2'", exception.getMessage());
    }

    @Test
    public void shouldThrowErrorIfHeaderHeaderPatternIsInvalid() {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(request);
        when(request.setBody("{\"key\": \"1\"}")).thenReturn(request);
        HashMap<String, String> staticHeader = new HashMap<String, String>();
        staticHeader.put("static", "3");
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "1", "{\"header_key_1\": \"%s\",\"header_key_2\": \"%p\"}", "1,2", "123", "234", false, null, "type", "345", staticHeader, null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), dynamicHeaderVariablesValues.toArray(), endpointVariablesValues.toArray());
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> httpPostRequestBuilder.create());
        assertEquals("pattern config '{\"header_key_1\": \"%s\",\"header_key_2\": \"%p\"}' is invalid", exception.getMessage());
    }
}
