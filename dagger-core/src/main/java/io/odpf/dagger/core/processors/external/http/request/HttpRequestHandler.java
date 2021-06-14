package io.odpf.dagger.core.processors.external.http.request;

import org.asynchttpclient.BoundRequestBuilder;

import java.util.Map;

/**
 * The interface Http request handler.
 */
public interface HttpRequestHandler {
    /**
     * Create bound request builder.
     *
     * @return the bound request builder
     */
    BoundRequestBuilder create();

    /**
     * Check if can create the request.
     *
     * @return the boolean
     */
    boolean canCreate();

    /**
     * Add headers bound request builder.
     *
     * @param request   the request
     * @param headerMap the header map
     * @return the bound request builder
     */
    default BoundRequestBuilder addHeaders(BoundRequestBuilder request, Map<String, String> headerMap) {
        headerMap.keySet().forEach(headerKey -> {
            request.addHeader(headerKey, headerMap.get(headerKey));
        });
        return request;
    }
}
