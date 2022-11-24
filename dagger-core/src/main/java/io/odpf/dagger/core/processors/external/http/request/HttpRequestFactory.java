package io.odpf.dagger.core.processors.external.http.request;

import io.odpf.dagger.core.exception.InvalidHttpVerbException;
import io.odpf.dagger.core.processors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

import java.util.ArrayList;

/**
 * The factoy class for Http request.
 */
public class HttpRequestFactory {
    /**
     * Create request bound request builder.
     *
     * @param httpSourceConfig       the http source config
     * @param httpClient             the http client
     * @param requestVariablesValues the request variables values
     * @return the bound request builder
     */
    public static BoundRequestBuilder createRequest(HttpSourceConfig httpSourceConfig, AsyncHttpClient httpClient, Object[] requestVariablesValues,  Object[] headerVariablesValues, Object[] endpointVariablesValues) {

        ArrayList<HttpRequestHandler> httpRequestHandlers = new ArrayList<>();
        httpRequestHandlers.add(new HttpPostRequestHandler(httpSourceConfig, httpClient, requestVariablesValues, headerVariablesValues, endpointVariablesValues));
        httpRequestHandlers.add(new HttpGetRequestHandler(httpSourceConfig, httpClient, requestVariablesValues, headerVariablesValues, endpointVariablesValues));
        httpRequestHandlers.add(new HttpPutRequestHandler(httpSourceConfig, httpClient, requestVariablesValues, headerVariablesValues, endpointVariablesValues));

        HttpRequestHandler httpRequestHandler = httpRequestHandlers
                .stream()
                .filter(HttpRequestHandler::canCreate)
                .findFirst()
                .orElseThrow(() -> new InvalidHttpVerbException("Http verb not supported"));
        return httpRequestHandler.create();
    }
}
