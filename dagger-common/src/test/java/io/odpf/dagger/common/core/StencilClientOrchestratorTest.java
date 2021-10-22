package io.odpf.dagger.common.core;

import org.apache.flink.api.java.utils.ParameterTool;

import com.gojek.de.stencil.client.ClassLoadStencilClient;
import com.gojek.de.stencil.client.MultiURLStencilClient;
import com.gojek.de.stencil.client.StencilClient;
import io.odpf.dagger.common.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.odpf.dagger.common.core.Constants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StencilClientOrchestratorTest {
    @Mock
    private ParameterTool parameterTool;

    private StencilClient stencilClient;
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
        this.configuration = new Configuration(parameterTool);
    }

    @Test
    public void shouldReturnClassLoadStencilClientIfStencilDisabled() throws NoSuchFieldException, IllegalAccessException {
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT);
        when(parameterTool.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_KEY, SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        stencilClient = stencilClientOrchestrator.getStencilClient();

        assertEquals(ClassLoadStencilClient.class, stencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldReturnMultiURLStencilClient() throws NoSuchFieldException, IllegalAccessException {
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT)).thenReturn("true");
        when(parameterTool.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn("http://localhost/latest,"
                + "http://localhost/events/latest,"
                + "http://localhost/entities/release");
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_KEY, SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        stencilClient = stencilClientOrchestrator.getStencilClient();

        assertEquals(MultiURLStencilClient.class, stencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldEnrichStencilClient() throws NoSuchFieldException, IllegalAccessException {
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT)).thenReturn("true");
        when(parameterTool.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_KEY, SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT);
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn("http://localhost/latest,");
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
        when(parameterTool.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT)).thenReturn("true");
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn("http://localhost/latest,");
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_KEY, SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT);
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
        when(parameterTool.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE_DEFAULT);
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
        when(parameterTool.get(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_KEY, SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        List<String> enrichmentStencilURLs = Collections
                .singletonList("http://localhost/latest");

        StencilClient enrichedStencilClient = stencilClientOrchestrator.enrichStencilClient(enrichmentStencilURLs);

        assertEquals(ClassLoadStencilClient.class, enrichedStencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }
}
