package com.gojek.daggers.postProcessors.external.http.request;

import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

public class HttpPostRequestBuilder implements HttpRequestBuilder {
    private HttpSourceConfig httpSourceConfig;
    private AsyncHttpClient httpClient;
    private Object[] requestVariablesValues;

    public HttpPostRequestBuilder(HttpSourceConfig httpSourceConfig, AsyncHttpClient httpClient, Object[] requestVariablesValues) {
        this.httpSourceConfig = httpSourceConfig;
        this.httpClient = httpClient;
        this.requestVariablesValues = requestVariablesValues;
    }

    @Override
    public BoundRequestBuilder build() {
        String requestBody = String.format(httpSourceConfig.getRequestPattern(), requestVariablesValues);
        String endpoint = httpSourceConfig.getEndpoint();
        BoundRequestBuilder postRequest = httpClient
                .preparePost(endpoint)
                .setBody(requestBody);
        return addHeaders(postRequest, httpSourceConfig.getHeaders());
    }

    @Override
    public boolean canBuild() {
        return httpSourceConfig.getVerb().equalsIgnoreCase("post");
    }
}
