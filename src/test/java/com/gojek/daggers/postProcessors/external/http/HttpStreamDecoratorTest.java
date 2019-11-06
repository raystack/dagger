package com.gojek.daggers.postProcessors.external.http;

import com.gojek.de.stencil.StencilClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpStreamDecoratorTest {
    @Mock
    private StencilClient stencilClient;

    @Mock
    private HttpSourceConfig httpSourceConfig;


    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void canDecorateHttpAsync() {
        HttpStreamDecorator httpStreamDecorator = new HttpStreamDecorator(httpSourceConfig, stencilClient, null);
        assertTrue(httpStreamDecorator.canDecorate());
    }

    @Test
    public void shouldNotDecorateOtherThanHttpAsync() {
        HttpStreamDecorator httpStreamDecorator = new HttpStreamDecorator(null, stencilClient, null);
        assertFalse(httpStreamDecorator.canDecorate());
    }
}