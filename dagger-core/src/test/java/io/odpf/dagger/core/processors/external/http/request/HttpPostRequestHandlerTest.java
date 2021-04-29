package io.odpf.dagger.core.processors.external.http.request;

import io.odpf.dagger.core.processors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpPostRequestHandlerTest {

    @Mock
    private AsyncHttpClient httpClient;

    @Mock
    private BoundRequestBuilder request;

    private HttpSourceConfig httpSourceConfig;
    private ArrayList<Object> requestVariablesValues;

    @Before
    public void setup() {
        initMocks(this);
        requestVariablesValues = new ArrayList<>();
        requestVariablesValues.add(1);
    }

    @Test
    public void shouldReturnTrueForPostVerbOnCanCreate() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "1", "123", "234", false, "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray());
        Assert.assertTrue(httpPostRequestBuilder.canCreate());
    }

    @Test
    public void shouldReturnFalseForVerbOtherThanPostOnCanBuild() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "GET", "{\"key\": \"%s\"}", "1", "123", "234", false, "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray());
        Assert.assertFalse(httpPostRequestBuilder.canCreate());
    }

    @Test
    public void shouldBuildPostRequest() {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(request);
        when(request.setBody("{\"key\": \"1\"}")).thenReturn(request);
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "1", "123", "234", false, "type", "345", new HashMap<>(), null, "metricId_01", false);
        HttpPostRequestHandler httpPostRequestBuilder = new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues.toArray());
        Assert.assertEquals(request, httpPostRequestBuilder.create());
    }

}
