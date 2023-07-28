package com.gotocompany.dagger.common.core;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.cache.SchemaRefreshStrategy;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static com.gotocompany.dagger.common.core.Constants.*;

/**
 * The Stencil client orchestrator for dagger.
 */
public class StencilClientOrchestrator implements Serializable {
    private static StencilClient stencilClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(StencilClientOrchestrator.class);
    private Configuration configuration;
    private HashSet<String> stencilUrls;

    /**
     * Instantiates a new Stencil client orchestrator.
     *
     * @param configuration the configuration
     */
    public StencilClientOrchestrator(Configuration configuration) {
        this.configuration = configuration;
        this.stencilUrls = getStencilUrls();
    }

    public StencilConfig createStencilConfig() {
        return StencilConfig.builder()
                .fetchHeaders(getHeaders(configuration))
                .fetchTimeoutMs(configuration.getInteger(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS, SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT))
                .cacheAutoRefresh(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT))
                .cacheTtlMs(configuration.getLong(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT))
                .refreshStrategy(getSchemaRefreshStrategy(configuration.getString(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT)))
                .fetchBackoffMinMs(configuration.getLong(SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS_KEY, SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS_DEFAULT))
                .fetchRetries(configuration.getInteger(SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES_KEY, SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES_DEFAULT))
                .build();
    }

    private SchemaRefreshStrategy getSchemaRefreshStrategy(String refreshStrategy) {
        if (refreshStrategy == null) {
            return SchemaRefreshStrategy.longPollingStrategy();
        }
        if (refreshStrategy.equalsIgnoreCase("VERSION_BASED_REFRESH")) {
            return SchemaRefreshStrategy.versionBasedRefresh();
        }
        return SchemaRefreshStrategy.longPollingStrategy();

    }

    private List<Header> getHeaders(Configuration config) {
        String headerString = config.getString(SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS_KEY, SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS_DEFAULT);
        return parseHeaders(headerString);
    }

    /**
     * Gets stencil client.
     *
     * @return the stencil client
     */
    public StencilClient getStencilClient() {

        if (stencilClient != null) {
            return stencilClient;
        }

        stencilClient = initStencilClient(new ArrayList<>(stencilUrls));
        return stencilClient;
    }

    /**
     * Enrich stencil client.
     *
     * @param additionalStencilUrls the additional stencil urls
     * @return the stencil client
     */
    public StencilClient enrichStencilClient(List<String> additionalStencilUrls) {
        if (additionalStencilUrls.isEmpty()) {
            return stencilClient;
        }

        stencilUrls.addAll(additionalStencilUrls);
        stencilClient = initStencilClient(new ArrayList<>(stencilUrls));
        return stencilClient;
    }

    private StencilClient initStencilClient(List<String> urls) {
        StencilConfig stencilConfig = createStencilConfig();
        boolean enableRemoteStencil = configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        return enableRemoteStencil
                ? StencilClientFactory.getClient(urls, stencilConfig)
                : StencilClientFactory.getClient();
    }

    private List<Header> parseHeaders(String headersString) {
        headersString = headersString == null ? "" : headersString;
        return Arrays.stream(headersString.split(","))
                .map(String::trim)
                .filter(this::isValidHeader)
                .map(this::parseHeader)
                .collect(Collectors.toList());
    }

    private Boolean isValidHeader(String headerString) {
        Boolean isValid = Arrays.stream(headerString.split(":")).map(String::trim).filter(a -> !a.isEmpty()).count() == 2;
        if (!isValid && !headerString.isEmpty()) {
            LOGGER.error("Invalid header {}. This will be ignored", headerString);
        }
        return isValid;
    }

    private BasicHeader parseHeader(String headerString) {
        String[] split = headerString.split(":");
        return new BasicHeader(split[0].trim(), split[1].trim());
    }

    private HashSet<String> getStencilUrls() {
        stencilUrls = Arrays.stream(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT).split(","))
                .map(String::trim)
                .collect(Collectors.toCollection(HashSet::new));
        return stencilUrls;
    }
}
