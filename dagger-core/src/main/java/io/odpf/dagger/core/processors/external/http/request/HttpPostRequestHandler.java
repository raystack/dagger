package io.odpf.dagger.core.processors.external.http.request;

import io.odpf.dagger.core.processors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

/**
 * The Http post request handler.
 */
public class HttpPostRequestHandler implements HttpRequestHandler {
    private HttpSourceConfig httpSourceConfig;
    private AsyncHttpClient httpClient;
    private Object[] requestVariablesValues;

    /**
     * Instantiates a new Http post request handler.
     *
     * @param httpSourceConfig       the http source config
     * @param httpClient             the http client
     * @param requestVariablesValues the request variables values
     */
    public HttpPostRequestHandler(HttpSourceConfig httpSourceConfig, AsyncHttpClient httpClient, Object[] requestVariablesValues) {
        this.httpSourceConfig = httpSourceConfig;
        this.httpClient = httpClient;
        this.requestVariablesValues = requestVariablesValues;
    }

    @Override
    public BoundRequestBuilder create() {
        String requestBody = String.format(httpSourceConfig.getPattern(), requestVariablesValues);
        String endpoint = httpSourceConfig.getEndpoint();
        BoundRequestBuilder postRequest = httpClient
                .preparePost(endpoint)
                .setBody(requestBody);
        return addHeaders(postRequest, httpSourceConfig.getHeaders());
    }

    @Override
    public boolean canCreate() {
        return httpSourceConfig.getVerb().equalsIgnoreCase("post");
    }
}
