package io.odpf.dagger.core.processors.external.http.request;

import com.google.gson.Gson;
import io.netty.util.internal.StringUtil;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.UnknownFormatConversionException;

/**
 * The Http get request handler.
 */
public class HttpGetRequestHandler implements HttpRequestHandler {
    private HttpSourceConfig httpSourceConfig;
    private AsyncHttpClient httpClient;
    private Object[] requestVariablesValues;
    private Object[] dynamicHeaderVariablesValues;

    /**
     * Instantiates a new Http get request handler.
     *
     * @param httpSourceConfig       the http source config
     * @param httpClient             the http client
     * @param requestVariablesValues the request variables values
     */
    public HttpGetRequestHandler(HttpSourceConfig httpSourceConfig, AsyncHttpClient httpClient, Object[] requestVariablesValues, Object[] dynamicHeaderVariablesValues) {
        this.httpSourceConfig = httpSourceConfig;
        this.httpClient = httpClient;
        this.requestVariablesValues = requestVariablesValues;
        this.dynamicHeaderVariablesValues = dynamicHeaderVariablesValues;
    }

    @Override
    public BoundRequestBuilder create() {
        String endpointPath = String.format(httpSourceConfig.getPattern(), requestVariablesValues);
        String endpoint = httpSourceConfig.getEndpoint();
        String requestEndpoint = endpoint + endpointPath;
        BoundRequestBuilder getRequest = httpClient.prepareGet(requestEndpoint);
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
        return addHeaders(getRequest, headers);
    }

    @Override
    public boolean canCreate() {
        return httpSourceConfig.getVerb().equalsIgnoreCase("get");
    }
}
