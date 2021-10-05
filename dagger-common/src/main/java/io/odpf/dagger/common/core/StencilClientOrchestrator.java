package io.odpf.dagger.common.core;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import io.odpf.dagger.common.configuration.UserConfiguration;

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
    private HashMap<String, String> stencilConfigMap;
    private HashSet<String> stencilUrls;
    private UserConfiguration userConfiguration;

    /**
     * Instantiates a new Stencil client orchestrator.
     *
     * @param userConfiguration the configuration
     */
    public StencilClientOrchestrator(UserConfiguration userConfiguration) {
        this.userConfiguration = userConfiguration;
        this.stencilConfigMap = createStencilConfigMap(userConfiguration);
        this.stencilUrls = getStencilUrls();
    }

    private HashMap<String, String> createStencilConfigMap(UserConfiguration userConfiguration) {
        stencilConfigMap = new HashMap<>();
        stencilConfigMap.put(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, userConfiguration.getParam().get(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT));
        stencilConfigMap.put(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_KEY, userConfiguration.getParam().get(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_KEY, SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT));
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
        boolean enableRemoteStencil = userConfiguration.getParam().getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        return enableRemoteStencil
                ? StencilClientFactory.getClient(urls, stencilConfigMap)
                : StencilClientFactory.getClient();
    }

    private HashSet<String> getStencilUrls() {
        stencilUrls = Arrays.stream(userConfiguration.getParam().get(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT).split(","))
                .map(String::trim)
                .collect(Collectors.toCollection(HashSet::new));
        return stencilUrls;
    }
}
