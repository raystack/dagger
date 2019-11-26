package com.gojek.daggers.postProcessors.external.http.request;

import org.asynchttpclient.BoundRequestBuilder;

import java.util.Map;

public interface HttpRequestBuilder {
    BoundRequestBuilder build();

    boolean canBuild();

    default BoundRequestBuilder addHeaders(BoundRequestBuilder request, Map<String, String> headerMap) {
        headerMap.keySet().forEach(headerKey -> {
            request.addHeader(headerKey, headerMap.get(headerKey));
        });
        return request;
    }
}
