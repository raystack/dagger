package com.gotocompany.dagger.core.source.config.adapter;

import com.google.gson.stream.JsonReader;
import com.gotocompany.dagger.core.exception.InvalidConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DaggerSSLProtocolAdaptorTest {

    @Mock
    private JsonReader jsonReader;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAcceptConfiguredValue() throws IOException {
        when(jsonReader.nextString()).thenReturn("SSL");
        DaggerSSLProtocolAdaptor daggerSSLProtocolAdaptor = new DaggerSSLProtocolAdaptor();
        String sslProtocol = daggerSSLProtocolAdaptor.read(jsonReader);
        assertEquals("SSL", sslProtocol);
    }

    @Test
    public void shouldNotAcceptValuesNotConfigured() throws IOException {
        when(jsonReader.nextString()).thenReturn("SSL1");
        DaggerSSLProtocolAdaptor daggerSSLProtocolAdaptor = new DaggerSSLProtocolAdaptor();
        assertThrows(InvalidConfigurationException.class, () -> daggerSSLProtocolAdaptor.read(jsonReader));
    }
}
