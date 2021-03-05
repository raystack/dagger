package com.gojek.daggers.postprocessors.external.http.request;

import org.asynchttpclient.BoundRequestBuilder;

import java.util.Map;

public interface HttpRequestHandler {
    BoundRequestBuilder create();

    boolean canCreate();

    default BoundRequestBuilder addHeaders(BoundRequestBuilder request, Map<String, String> headerMap) {
        headerMap.keySet().forEach(headerKey -> {
            request.addHeader(headerKey, headerMap.get(headerKey));
        });
        return request;
    }
}
