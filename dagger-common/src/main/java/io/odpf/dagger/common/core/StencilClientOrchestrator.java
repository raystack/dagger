package io.odpf.dagger.common.core;

import org.apache.flink.configuration.Configuration;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static io.odpf.dagger.common.core.Constants.*;

/**
 * The Stencil client orchestrator for dagger.
 */
public class StencilClientOrchestrator implements Serializable {
    private static StencilClient stencilClient;
    private Configuration configuration;
    private HashMap<String, String> stencilConfigMap;
    private HashSet<String> stencilUrls;

    /**
     * Instantiates a new Stencil client orchestrator.
     *
     * @param configuration the configuration
     */
    public StencilClientOrchestrator(Configuration configuration) {
        this.configuration = configuration;
        this.stencilConfigMap = createStencilConfigMap(configuration);
        this.stencilUrls = getStencilUrls();
    }

    private HashMap<String, String> createStencilConfigMap(Configuration config) {
        stencilConfigMap = new HashMap<>();
        stencilConfigMap.put(STENCIL_CONFIG_REFRESH_CACHE_KEY, config.getString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT));
        stencilConfigMap.put(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, config.getString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT));
        stencilConfigMap.put(STENCIL_CONFIG_TIMEOUT_MS_KEY, config.getString(STENCIL_CONFIG_TIMEOUT_MS_KEY, STENCIL_CONFIG_TIMEOUT_MS_DEFAULT));
        return stencilConfigMap;
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
        boolean enableRemoteStencil = configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT);
        return enableRemoteStencil
                ? StencilClientFactory.getClient(urls, stencilConfigMap)
                : StencilClientFactory.getClient();
    }

    private HashSet<String> getStencilUrls() {
        stencilUrls = Arrays.stream(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT).split(","))
                .map(String::trim)
                .collect(Collectors.toCollection(HashSet::new));
        return stencilUrls;
    }
}
