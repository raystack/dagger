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

public class HttpGetRequestHandlerTest {
    @Mock
    private AsyncHttpClient httpClient;

    @Mock
    private BoundRequestBuilder request;

    private HttpSourceConfig httpSourceConfig;
    private ArrayList<Object> requestVariablesValues;
    private ArrayList<Object> headerVariablesValues;
    private ArrayList<Object> endpointVariablesValues;

    @Before
    public void setup() {
        initMocks(this);
        requestVariablesValues = new ArrayList<>();
        requestVariablesValues.add(1);
        headerVariablesValues = new ArrayList<>();
        headerVariablesValues.add("1");
        headerVariablesValues.add("2");
        endpointVariablesValues = new ArrayList<>();
    }

    @Test
    public void shouldReturnTrueForGetVerbOnCanCreate() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "GET", "{\"key\": \"%s\"}", "1", "", "", "123", "234", false, "", "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpGetRequestHandler httpGetRequestBuilder = new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());
        assertTrue(httpGetRequestBuilder.canCreate());
    }

    @Test
    public void shouldReturnFalseForVerbOtherThanGetOnCanBuild() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "POST", "{\"key\": \"%s\"}", "1", "", "", "123", "234", false, "", "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpGetRequestHandler httpGetRequestBuilder = new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());
        assertFalse(httpGetRequestBuilder.canCreate());
    }

    @Test
    public void shouldBuildGetRequest() {
        when(httpClient.prepareGet("http://localhost:8080/test/key/1")).thenReturn(request);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "GET", "/key/%s", "1", "", "", "123", "234", false, "", "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpGetRequestHandler httpGetRequestBuilder = new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());
        assertEquals(request, httpGetRequestBuilder.create());
    }

    @Test
    public void shouldBuildGetRequestWithOnlyDynamicHeader() {
        when(httpClient.prepareGet("http://localhost:8080/test/key/1")).thenReturn(request);
        when(request.addHeader("header_key", "1")).thenReturn(request);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "GET", "/key/%s", "1", "{\"header_key\": \"%s\"}", "1", "123", "234", false, "", "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpGetRequestHandler httpGetRequestBuilder = new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());
        httpGetRequestBuilder.create();
        verify(request, times(1)).addHeader(anyString(), anyString());
        verify(request, times(1)).addHeader("header_key", "1");
    }

    @Test
    public void shouldBuildGetRequestWithDynamicAndStaticHeader() {
        when(httpClient.prepareGet("http://localhost:8080/test/key/1")).thenReturn(request);
        when(request.addHeader("header_key", "1")).thenReturn(request);
        HashMap<String, String> staticHeader = new HashMap<String, String>();
        staticHeader.put("static", "2");
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "GET", "/key/%s", "1", "{\"header_key\": \"%s\"}", "1", "123", "234", false, "", "type", "345", staticHeader, null, "metricId_01", false);
        HttpGetRequestHandler httpGetRequestBuilder = new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());
        httpGetRequestBuilder.create();
        verify(request, times(2)).addHeader(anyString(), anyString());
        verify(request, times(1)).addHeader("header_key", "1");
        verify(request, times(1)).addHeader("static", "2");
    }

    @Test
    public void shouldBuildGetRequestWithMultipleDynamicAndStaticHeaders() {
        when(httpClient.prepareGet("http://localhost:8080/test/key/1")).thenReturn(request);
        HashMap<String, String> staticHeader = new HashMap<String, String>();
        staticHeader.put("static", "3");
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "GET", "/key/%s", "1", "{\"header_key_1\": \"%s\",\"header_key_2\": \"%s\"}", "1,2", "123", "234", false, "", "type", "345", staticHeader, null, "metricId_01", false);
        HttpGetRequestHandler httpGetRequestBuilder = new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());
        httpGetRequestBuilder.create();
        verify(request, times(3)).addHeader(anyString(), anyString());
        verify(request, times(1)).addHeader("header_key_1", "1");
        verify(request, times(1)).addHeader("header_key_2", "2");
        verify(request, times(1)).addHeader("static", "3");
    }

    @Test
    public void shouldThrowErrorIfHeaderVariablesAreIncompatible() {
        when(httpClient.prepareGet("http://localhost:8080/test/key/1")).thenReturn(request);
        HashMap<String, String> staticHeader = new HashMap<String, String>();
        staticHeader.put("static", "3");
        ArrayList incompatibleHeaderVariablesValues = new ArrayList<>();
        incompatibleHeaderVariablesValues.add("test1");
        incompatibleHeaderVariablesValues.add("test12");
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "GET", "/key/%s", "1", "{\"header_key_1\": \"%s\",\"header_key_2\": \"%d\"}", "1,2", "123", "234", false, "", "type", "345", staticHeader, null, "metricId_01", false);
        HttpGetRequestHandler httpGetRequestBuilder = new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), incompatibleHeaderVariablesValues.toArray(), endpointVariablesValues.toArray());
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> httpGetRequestBuilder.create());
        assertEquals("pattern config '{\"header_key_1\": \"%s\",\"header_key_2\": \"%d\"}' is incompatible with the variable config '1,2'", exception.getMessage());
    }

    @Test
    public void shouldThrowErrorIfHeaderHeaderPatternIsInvalid() {
        when(httpClient.prepareGet("http://localhost:8080/test/key/1")).thenReturn(request);
        HashMap<String, String> staticHeader = new HashMap<String, String>();
        staticHeader.put("static", "3");
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "", "GET", "/key/%s", "1", "{\"header_key_1\": \"%s\",\"header_key_2\": \"%p\"}", "1,2", "123", "234", false, "", "type", "345", staticHeader, null, "metricId_01", false);
        HttpGetRequestHandler httpGetRequestBuilder = new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray(), headerVariablesValues.toArray(), endpointVariablesValues.toArray());
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> httpGetRequestBuilder.create());
        assertEquals("pattern config '{\"header_key_1\": \"%s\",\"header_key_2\": \"%p\"}' is invalid", exception.getMessage());
    }
}
