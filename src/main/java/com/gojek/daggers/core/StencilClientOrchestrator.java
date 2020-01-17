package com.gojek.daggers.core;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.gojek.daggers.utils.Constants.*;

public class StencilClientOrchestrator implements Serializable {
    private static StencilClient stencilClient;
    private Configuration configuration;
    private HashMap<String, String> stencilConfigMap;

    public StencilClientOrchestrator(Configuration configuration) {
        this.configuration = configuration;
        this.stencilConfigMap = createStencilConfigMap(configuration);
    }

    private HashMap<String, String> createStencilConfigMap(Configuration configuration) {
        stencilConfigMap = new HashMap<>();
        stencilConfigMap.put(REFRESH_CACHE_KEY, configuration.getString(REFRESH_CACHE_KEY, REFRESH_CACHE_DEFAULT));
        stencilConfigMap.put(TTL_IN_MINUTES_KEY, configuration.getString(TTL_IN_MINUTES_KEY, TTL_IN_MINUTES_DEFAULT));
        return stencilConfigMap;
    }

    public StencilClient getStencilClient() {
        if (stencilClient != null) return stencilClient;
        boolean enableRemoteStencil = configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT);
        List<String> stencilUrls = Arrays.stream(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT).split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        stencilClient = enableRemoteStencil
                ? StencilClientFactory.getClient(stencilUrls, stencilConfigMap)
                : StencilClientFactory.getClient();
        return stencilClient;
    }
}
