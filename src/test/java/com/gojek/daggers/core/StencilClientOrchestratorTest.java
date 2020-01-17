package com.gojek.daggers.core;

import com.gojek.de.stencil.client.ClassLoadStencilClient;
import com.gojek.de.stencil.client.MultiURLStencilClient;
import com.gojek.de.stencil.client.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.lang.reflect.Field;

import static com.gojek.daggers.utils.Constants.*;
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
        when(configuration.getString(REFRESH_CACHE_KEY, REFRESH_CACHE_DEFAULT)).thenReturn(REFRESH_CACHE_DEFAULT);
        when(configuration.getString(TTL_IN_MINUTES_KEY, TTL_IN_MINUTES_DEFAULT)).thenReturn(TTL_IN_MINUTES_DEFAULT);
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT)).thenReturn(STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT)).thenReturn(STENCIL_URL_DEFAULT);
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        stencilClient = stencilClientOrchestrator.getStencilClient();

        Assert.assertEquals(ClassLoadStencilClient.class, stencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }

    @Test
    public void shouldReturnMultiURLStencilClient() throws NoSuchFieldException, IllegalAccessException {
        when(configuration.getString(REFRESH_CACHE_KEY, REFRESH_CACHE_DEFAULT)).thenReturn("true");
        when(configuration.getString(TTL_IN_MINUTES_KEY, TTL_IN_MINUTES_DEFAULT)).thenReturn("30");
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT)).thenReturn("http://artifactory-gojek.golabs.io/artifactory/proto-descriptors/esb-log-entities/latest," +
                "http://artifactory-gojek.golabs.io/artifactory/proto-descriptors/goid-events/latest," +
                "http://artifactory-gojek.golabs.io/artifactory/proto-descriptors/growth-log-entities/release");
        StencilClientOrchestrator stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        stencilClient = stencilClientOrchestrator.getStencilClient();

        Assert.assertEquals(MultiURLStencilClient.class, stencilClient.getClass());
        Field stencilClientField = StencilClientOrchestrator.class.getDeclaredField("stencilClient");
        stencilClientField.setAccessible(true);
        stencilClientField.set(null, null);
    }
}