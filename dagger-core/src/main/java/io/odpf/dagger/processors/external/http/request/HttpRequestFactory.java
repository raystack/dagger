package io.odpf.dagger.processors.external.http.request;

import io.odpf.dagger.exception.InvalidHttpVerbException;
import io.odpf.dagger.processors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

import java.util.ArrayList;

public class HttpRequestFactory {
    public static BoundRequestBuilder createRequest(HttpSourceConfig httpSourceConfig, AsyncHttpClient httpClient, Object[] requestVariablesValues) {

        ArrayList<HttpRequestHandler> httpRequestHandlers = new ArrayList<>();
        httpRequestHandlers.add(new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues));
        httpRequestHandlers.add(new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues));

        HttpRequestHandler httpRequestHandler = httpRequestHandlers
                .stream()
                .filter(HttpRequestHandler::canCreate)
                .findFirst()
                .orElseThrow(() -> new InvalidHttpVerbException("Http verb not supported"));
        return httpRequestHandler.create();
    }
}