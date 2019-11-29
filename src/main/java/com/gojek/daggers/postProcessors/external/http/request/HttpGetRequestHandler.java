package com.gojek.daggers.postProcessors.external.http.request;

import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

public class HttpGetRequestHandler implements HttpRequestHandler {
    private HttpSourceConfig httpSourceConfig;
    private AsyncHttpClient httpClient;
    private Object[] requestVariablesValues;

    public HttpGetRequestHandler(HttpSourceConfig httpSourceConfig, AsyncHttpClient httpClient, Object[] requestVariablesValues) {
        this.httpSourceConfig = httpSourceConfig;
        this.httpClient = httpClient;
        this.requestVariablesValues = requestVariablesValues;
    }

    @Override
    public BoundRequestBuilder create() {
        String endpointPath = String.format(httpSourceConfig.getRequestPattern(), requestVariablesValues);
        String endpoint = httpSourceConfig.getEndpoint();
        String requestEndpoint = endpoint + endpointPath;
        BoundRequestBuilder getRequest = httpClient.prepareGet(requestEndpoint);
        return addHeaders(getRequest, httpSourceConfig.getHeaders());
    }

    @Override
    public boolean canCreate() {
        return httpSourceConfig.getVerb().equalsIgnoreCase("get");
    }
}