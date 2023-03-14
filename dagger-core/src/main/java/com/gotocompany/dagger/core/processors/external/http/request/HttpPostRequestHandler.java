package com.gotocompany.dagger.core.processors.external.http.request;

import com.google.gson.Gson;
import com.gotocompany.dagger.core.exception.InvalidConfigurationException;
import io.netty.util.internal.StringUtil;
import com.gotocompany.dagger.core.processors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.UnknownFormatConversionException;

/**
 * The Http post request handler.
 */
public class HttpPostRequestHandler implements HttpRequestHandler {
    private HttpSourceConfig httpSourceConfig;
    private AsyncHttpClient httpClient;
    private Object[] requestVariablesValues;
    private Object[] dynamicHeaderVariablesValues;
    private Object[] endpointVariablesValues;
    /**
     * Instantiates a new Http post request handler.
     *
     * @param httpSourceConfig        the http source config
     * @param httpClient              the http client
     * @param requestVariablesValues  the request variables values
     * @param endpointVariablesValues the endpoint variables values
     */
    public HttpPostRequestHandler(HttpSourceConfig httpSourceConfig, AsyncHttpClient httpClient, Object[] requestVariablesValues, Object[] dynamicHeaderVariablesValues, Object[] endpointVariablesValues) {
        this.httpSourceConfig = httpSourceConfig;
        this.httpClient = httpClient;
        this.requestVariablesValues = requestVariablesValues;
        this.dynamicHeaderVariablesValues = dynamicHeaderVariablesValues;
        this.endpointVariablesValues = endpointVariablesValues;
    }

    @Override
    public BoundRequestBuilder create() {
        String requestBody = String.format(httpSourceConfig.getPattern(), requestVariablesValues);
        String endpoint = String.format(httpSourceConfig.getEndpoint(), endpointVariablesValues);

        BoundRequestBuilder postRequest = httpClient
                .preparePost(endpoint)
                .setBody(requestBody);
        Map<String, String> headers = httpSourceConfig.getHeaders();
        if (!StringUtil.isNullOrEmpty(httpSourceConfig.getHeaderPattern())) {
            try {
                String dynamicHeader = String.format(httpSourceConfig.getHeaderPattern(), dynamicHeaderVariablesValues);
                headers.putAll(new Gson().fromJson(dynamicHeader, HashMap.class));
            } catch (UnknownFormatConversionException e) {
                throw new InvalidConfigurationException(String.format("pattern config '%s' is invalid", httpSourceConfig.getHeaderPattern()));
            } catch (IllegalArgumentException e) {
                throw new InvalidConfigurationException(String.format("pattern config '%s' is incompatible with the variable config '%s'", httpSourceConfig.getHeaderPattern(), httpSourceConfig.getHeaderVariables()));
            }
        }
        return addHeaders(postRequest, headers);
    }

    @Override
    public boolean canCreate() {
        return httpSourceConfig.getVerb().equalsIgnoreCase("post");
    }
}
