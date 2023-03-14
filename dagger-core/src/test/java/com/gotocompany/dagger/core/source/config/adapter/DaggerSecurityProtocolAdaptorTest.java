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

public class DaggerSecurityProtocolAdaptorTest {

    @Mock
    private JsonReader jsonReader;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAcceptConfiguredValue() throws IOException {
        when(jsonReader.nextString()).thenReturn("SASL_PLAINTEXT");
        DaggerSecurityProtocolAdaptor daggerSecurityProtocolAdaptor = new DaggerSecurityProtocolAdaptor();
        String securityProtocol = daggerSecurityProtocolAdaptor.read(jsonReader);
        assertEquals("SASL_PLAINTEXT", securityProtocol);
    }

    @Test
    public void shouldNotAcceptConfiguredValue() throws IOException {
        when(jsonReader.nextString()).thenReturn("SASLPLAINTEXT");
        DaggerSecurityProtocolAdaptor daggerSecurityProtocolAdaptor = new DaggerSecurityProtocolAdaptor();
        assertThrows(InvalidConfigurationException.class, () -> daggerSecurityProtocolAdaptor.read(jsonReader));
    }
}
