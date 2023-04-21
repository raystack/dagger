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

public class DaggerSSLTrustStoreFileTypeAdaptorTest {

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
        DaggerSSLTrustStoreFileTypeAdaptor daggerSSLTrustStoreFileTypeAdaptor = new DaggerSSLTrustStoreFileTypeAdaptor();
        String truststoreFileType = daggerSSLTrustStoreFileTypeAdaptor.read(jsonReader);
        assertEquals(storeFileType, truststoreFileType);
    }

    @Test
    public void shouldNotAcceptValuesNotConfigured() throws IOException {
        when(jsonReader.nextString()).thenReturn("JKS12");
        DaggerSSLTrustStoreFileTypeAdaptor daggerSSLTrustStoreFileTypeAdaptor = new DaggerSSLTrustStoreFileTypeAdaptor();
        assertThrows(InvalidConfigurationException.class, () -> daggerSSLTrustStoreFileTypeAdaptor.read(jsonReader));
    }
}
