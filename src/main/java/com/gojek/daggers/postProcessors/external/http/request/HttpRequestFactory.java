package com.gojek.daggers.postProcessors.external.http.request;

import com.gojek.daggers.exception.InvalidHttpVerbException;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

import java.util.ArrayList;

public class HttpRequestFactory {
    public static BoundRequestBuilder createRequest(HttpSourceConfig httpSourceConfig, AsyncHttpClient httpClient, Object[] requestVariablesValues) {

        ArrayList<HttpRequestBuilder> httpRequestBuilders = new ArrayList<>();
        httpRequestBuilders.add(new HttpPostRequestBuilder(httpSourceConfig, httpClient, requestVariablesValues));
        httpRequestBuilders.add(new HttpGetRequestBuilder(httpSourceConfig, httpClient, requestVariablesValues));

        HttpRequestBuilder httpRequestBuilder = httpRequestBuilders
                .stream()
                .filter(HttpRequestBuilder::canBuild)
                .findFirst()
                .orElseThrow(() -> new InvalidHttpVerbException("Http verb not supported"));
        return httpRequestBuilder.build();
    }
}
