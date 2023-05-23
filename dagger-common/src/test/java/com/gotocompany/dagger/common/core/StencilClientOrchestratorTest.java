package com.gotocompany.dagger.common.core;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.stencil.cache.SchemaRefreshStrategy;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import com.gotocompany.stencil.client.MultiURLStencilClient;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.lang.reflect.Field;
import java.util.*;

import static com.gotocompany.dagger.common.core.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StencilClientOrchestratorTest {
    private StencilClient stencilClient;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    private Configuration getConfig(Map<String, String> mapConfig) {
        return new Configuration(ParameterTool.fromMap(mapConfig));
    }

    @Test
    public void shouldReturnClassLoadStencilClientIfStencilDisabled() throws NoSuchFieldException, IllegalAccessException {
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
        when(configuration.getInteger(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS, SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT);
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT);
        when(configuration.getLong(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        stencilClient = stencilClientOrchestrator.getStencilClient();

        assertEquals(ClassLoadStencilClient.class, stencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldReturnMultiURLStencilClient() throws NoSuchFieldException, IllegalAccessException {
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn("http://localhost/latest,"
                + "http://localhost/events/latest,"
                + "http://localhost/entities/release");
        when(configuration.getInteger(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS, SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT);
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT);
        when(configuration.getLong(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        stencilClient = stencilClientOrchestrator.getStencilClient();

        assertEquals(MultiURLStencilClient.class, stencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldEnrichStencilClient() throws NoSuchFieldException, IllegalAccessException {
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getInteger(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS, SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn("http://localhost/latest,");
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT);
        when(configuration.getLong(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        StencilClient oldStencilClient = stencilClientOrchestrator.getStencilClient();


        List<String> enrichmentStencilURLs = Collections
                .singletonList("http://localhost/latest");

        assertSame(oldStencilClient, stencilClientOrchestrator.getStencilClient());

        StencilClient enrichedStencilClient = stencilClientOrchestrator.enrichStencilClient(enrichmentStencilURLs);

        assertNotSame(oldStencilClient, enrichedStencilClient);
        assertSame(enrichedStencilClient, stencilClientOrchestrator.getStencilClient());


        assertEquals(MultiURLStencilClient.class, enrichedStencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldNotEnrichIfNoNewAdditionalURLsAdded() throws NoSuchFieldException, IllegalAccessException {
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn("http://localhost/latest,");
        when(configuration.getInteger(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS, SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT);
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT);
        when(configuration.getLong(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        StencilClient oldStencilClient = stencilClientOrchestrator.getStencilClient();


        List<String> enrichmentStencilURLs = new ArrayList<>();

        assertSame(oldStencilClient, stencilClientOrchestrator.getStencilClient());

        StencilClient enrichedStencilClient = stencilClientOrchestrator.enrichStencilClient(enrichmentStencilURLs);

        assertSame(oldStencilClient, enrichedStencilClient);
        assertSame(enrichedStencilClient, stencilClientOrchestrator.getStencilClient());


        assertEquals(MultiURLStencilClient.class, enrichedStencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldReturnClassLoadStencilClientWhenStencilDisabledAndEnrichmentStencilUrlsIsNotNull() throws NoSuchFieldException, IllegalAccessException {
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
        when(configuration.getInteger(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS, SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT);
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT);
        when(configuration.getLong(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        List<String> enrichmentStencilURLs = Collections
                .singletonList("http://localhost/latest");

        StencilClient enrichedStencilClient = stencilClientOrchestrator.enrichStencilClient(enrichmentStencilURLs);

        assertEquals(ClassLoadStencilClient.class, enrichedStencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldReturnDefaultTimeoutIfTimeoutMsConfigNotSet() {
        Map<String, String> configMap = new HashMap<>();
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(Integer.valueOf(10000), stencilConfig.getFetchTimeoutMs());
    }

    @Test
    public void shouldReturnConfiguredTimeoutIfTimeoutMsConfigIsSet() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS, "8000");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(Integer.valueOf(8000), stencilConfig.getFetchTimeoutMs());
    }

    @Test
    public void shouldReturnTrueIfCacheAutoRefreshIsSetToTrue() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, "true");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertTrue(stencilConfig.getCacheAutoRefresh());
    }

    @Test
    public void shouldReturnFalseIfCacheAutoRefreshIsSetToFalse() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, "false");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertFalse(stencilConfig.getCacheAutoRefresh());
    }

    @Test
    public void shouldReturnFalseIfCacheAutoRefreshIsNotSet() {
        Map<String, String> configMap = new HashMap<>();

        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertFalse(stencilConfig.getCacheAutoRefresh());
    }

    @Test
    public void shouldReturnConfiguredValueIfCacheTtlMsConfigIsSet() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_KEY, "7800");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(Long.valueOf(7800), stencilConfig.getCacheTtlMs());
    }

    @Test
    public void shouldReturnDefaultValueIfCacheTtlMsConfigIsNotSet() {
        Map<String, String> configMap = new HashMap<>();

        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(Long.valueOf(900000), stencilConfig.getCacheTtlMs());
    }

    @Test
    public void shouldReturnVersionBasedIfRefreshStrategyConfigIsSet() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY, "VERSION_BASED_REFRESH");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(SchemaRefreshStrategy.versionBasedRefresh().getClass(), stencilConfig.getRefreshStrategy().getClass());
    }

    @Test
    public void shouldReturnLongPollingIfRefreshStrategyConfigIsNotSet() {
        Map<String, String> configMap = new HashMap<>();

        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(SchemaRefreshStrategy.longPollingStrategy().getClass(), stencilConfig.getRefreshStrategy().getClass());
    }

    @Test
    public void shouldReturnLongPollingIfRefreshStrategyConfigIsInvalid() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY, "xyz");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(SchemaRefreshStrategy.longPollingStrategy().getClass(), stencilConfig.getRefreshStrategy().getClass());
    }

    @Test
    public void shouldReturnConfiguredValueIfFetchBackoffMinMsConfigIsSet() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS_KEY, "7800");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(Long.valueOf(7800), stencilConfig.getFetchBackoffMinMs());
    }

    @Test
    public void shouldReturnDefaultValueIfFetchBackoffMinMsConfigIsNotSet() {
        Map<String, String> configMap = new HashMap<>();

        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(Long.valueOf(60000), stencilConfig.getFetchBackoffMinMs());
    }

    @Test
    public void shouldReturnConfiguredValueIfFetchRetriesConfigIsSet() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES_KEY, "9");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(Integer.valueOf(9), stencilConfig.getFetchRetries());
    }

    @Test
    public void shouldReturnDefaultValueIfFetchRetriesConfigIsNotSet() {
        Map<String, String> configMap = new HashMap<>();

        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(Integer.valueOf(4), stencilConfig.getFetchRetries());
    }

    @Test
    public void shouldReturnEmptyHeadersIfHeadersConfigIsNotSet() {
        Map<String, String> configMap = new HashMap<>();
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(0, stencilConfig.getFetchHeaders().size());
    }

    @Test
    public void shouldReturnEmptyHeadersIfHeadersConfigIsEmpty() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS_KEY, "");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(0, stencilConfig.getFetchHeaders().size());
    }

    @Test
    public void shouldReturnEmptyHeadersIfKeyValuePairsNotValid() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS_KEY, " : , : ::: : ,, :: , keyWithoutValue: , : valueWithoutKey, k1 : v1 : v2 ,, ");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(0, stencilConfig.getFetchHeaders().size());
    }

    @Test
    public void shouldReturnParsedHeaderIfHeaderStringValid() {
        Map<String, String> configMap = new HashMap<String, String>() {{
            put(SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS_KEY, " key1:val1 , key2 : val2 ,key3:val3");
        }};
        Configuration config = getConfig(configMap);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(config);
        StencilConfig stencilConfig = stencilClientOrchestrator.createStencilConfig();
        assertEquals(3, stencilConfig.getFetchHeaders().size());
        assertEquals("key1: val1", stencilConfig.getFetchHeaders().get(0).toString());
        assertEquals("key2: val2", stencilConfig.getFetchHeaders().get(1).toString());
        assertEquals("key3: val3", stencilConfig.getFetchHeaders().get(2).toString());
    }

}
