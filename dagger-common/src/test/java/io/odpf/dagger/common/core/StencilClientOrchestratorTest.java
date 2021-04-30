package io.odpf.dagger.common.core;

import com.gojek.de.stencil.client.ClassLoadStencilClient;
import com.gojek.de.stencil.client.MultiURLStencilClient;
import com.gojek.de.stencil.client.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.odpf.dagger.common.core.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StencilClientOrchestratorTest {
    @Mock
    private Configuration configuration;

    private StencilClient stencilClient;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnClassLoadStencilClientIfStencilDisabled() throws NoSuchFieldException, IllegalAccessException {
        when(configuration.getString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT)).thenReturn(STENCIL_CONFIG_REFRESH_CACHE_DEFAULT);
        when(configuration.getString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT)).thenReturn(STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT);
        when(configuration.getString(STENCIL_CONFIG_TIMEOUT_MS_KEY, STENCIL_CONFIG_TIMEOUT_MS_DEFAULT)).thenReturn(STENCIL_CONFIG_TIMEOUT_MS_DEFAULT);
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT)).thenReturn(STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT)).thenReturn(STENCIL_URL_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        stencilClient = stencilClientOrchestrator.getStencilClient();

        assertEquals(ClassLoadStencilClient.class, stencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldReturnMultiURLStencilClient() throws NoSuchFieldException, IllegalAccessException {
        when(configuration.getString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT)).thenReturn("true");
        when(configuration.getString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT)).thenReturn("30");
        when(configuration.getString(STENCIL_CONFIG_TIMEOUT_MS_KEY, STENCIL_CONFIG_TIMEOUT_MS_DEFAULT)).thenReturn(STENCIL_CONFIG_TIMEOUT_MS_DEFAULT);
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT)).thenReturn("http://localhost/latest," +
                "http://localhost/events/latest," +
                "http://localhost/entities/release");
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        stencilClient = stencilClientOrchestrator.getStencilClient();

        assertEquals(MultiURLStencilClient.class, stencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldEnrichStencilClient() throws NoSuchFieldException, IllegalAccessException {
        when(configuration.getString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT)).thenReturn("true");
        when(configuration.getString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT)).thenReturn("30");
        when(configuration.getString(STENCIL_CONFIG_TIMEOUT_MS_KEY, STENCIL_CONFIG_TIMEOUT_MS_DEFAULT)).thenReturn(STENCIL_CONFIG_TIMEOUT_MS_DEFAULT);
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT)).thenReturn("http://localhost/latest,");
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
        when(configuration.getString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT)).thenReturn("true");
        when(configuration.getString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT)).thenReturn("30");
        when(configuration.getString(STENCIL_CONFIG_TIMEOUT_MS_KEY, STENCIL_CONFIG_TIMEOUT_MS_DEFAULT)).thenReturn(STENCIL_CONFIG_TIMEOUT_MS_DEFAULT);
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT)).thenReturn("http://localhost/latest,");
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
        when(configuration.getString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT)).thenReturn(STENCIL_CONFIG_REFRESH_CACHE_DEFAULT);
        when(configuration.getString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT)).thenReturn(STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT);
        when(configuration.getString(STENCIL_CONFIG_TIMEOUT_MS_KEY, STENCIL_CONFIG_TIMEOUT_MS_DEFAULT)).thenReturn(STENCIL_CONFIG_TIMEOUT_MS_DEFAULT);
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT)).thenReturn(STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT)).thenReturn(STENCIL_URL_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        List<String> enrichmentStencilURLs = Collections
                .singletonList("http://localhost/latest");

        StencilClient stencilClient = stencilClientOrchestrator.enrichStencilClient(enrichmentStencilURLs);

        assertEquals(ClassLoadStencilClient.class, stencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }
}