package io.odpf.dagger.core.processors.external.http.request;

import io.odpf.dagger.core.processors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

/**
 * The Http get request handler.
 */
public class HttpGetRequestHandler implements HttpRequestHandler {
    private HttpSourceConfig httpSourceConfig;
    private AsyncHttpClient httpClient;
    private Object[] requestVariablesValues;

    /**
     * Instantiates a new Http get request handler.
     *
     * @param httpSourceConfig       the http source config
     * @param httpClient             the http client
     * @param requestVariablesValues the request variables values
     */
    public HttpGetRequestHandler(HttpSourceConfig httpSourceConfig, AsyncHttpClient httpClient, Object[] requestVariablesValues) {
        this.httpSourceConfig = httpSourceConfig;
        this.httpClient = httpClient;
        this.requestVariablesValues = requestVariablesValues;
    }

    @Override
    public BoundRequestBuilder create() {
        String endpointPath = String.format(httpSourceConfig.getPattern(), requestVariablesValues);
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
