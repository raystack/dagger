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

public class DaggerSASLMechanismAdaptorTest {
    @Mock
    private JsonReader jsonReader;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAcceptConfiguredValue() throws IOException {
        when(jsonReader.nextString()).thenReturn("SCRAM-SHA-512");
        DaggerSASLMechanismAdaptor daggerSASLMechanismAdaptor = new DaggerSASLMechanismAdaptor();
        String saslMechanism = daggerSASLMechanismAdaptor.read(jsonReader);
        assertEquals("SCRAM-SHA-512", saslMechanism);
    }

    @Test
    public void shouldNotAcceptConfiguredValue() throws IOException {
        when(jsonReader.nextString()).thenReturn("SCRAMSHA512");
        DaggerSASLMechanismAdaptor daggerSASLMechanismAdaptor = new DaggerSASLMechanismAdaptor();
        assertThrows(InvalidConfigurationException.class, () -> daggerSASLMechanismAdaptor.read(jsonReader));
    }
}
