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

public class DaggerSSLKeyStoreFileTypeAdaptorTest {

    @Mock
    private JsonReader jsonReader;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAcceptConfiguredValue() throws IOException {
        String storeFileType = "JKS";
        when(jsonReader.nextString()).thenReturn(storeFileType);
        DaggerSSLKeyStoreFileTypeAdaptor daggerSSLKeyStoreFileTypeAdaptor = new DaggerSSLKeyStoreFileTypeAdaptor();
        String keystoreFileType = daggerSSLKeyStoreFileTypeAdaptor.read(jsonReader);
        assertEquals(storeFileType, keystoreFileType);
    }

    @Test
    public void shouldNotAcceptValuesNotConfigured1() throws IOException {
        when(jsonReader.nextString()).thenReturn("JKS12");
        DaggerSSLKeyStoreFileTypeAdaptor daggerSSLKeyStoreFileTypeAdaptor = new DaggerSSLKeyStoreFileTypeAdaptor();
        assertThrows(InvalidConfigurationException.class, () -> daggerSSLKeyStoreFileTypeAdaptor.read(jsonReader));
    }

    @Test
    public void shouldNotAcceptValuesNotConfigured2() throws IOException {
        when(jsonReader.nextString()).thenReturn("");
        DaggerSSLKeyStoreFileTypeAdaptor daggerSSLKeyStoreFileTypeAdaptor = new DaggerSSLKeyStoreFileTypeAdaptor();
        assertThrows(InvalidConfigurationException.class, () -> daggerSSLKeyStoreFileTypeAdaptor.read(jsonReader));
    }
}
