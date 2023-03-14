package com.gotocompany.dagger.core.processors;

import com.gotocompany.dagger.common.core.DaggerContextTestBase;
import com.gotocompany.dagger.core.metrics.telemetry.TelemetrySubscriber;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParentPostProcessorTest extends DaggerContextTestBase {

    @Mock
    private TelemetrySubscriber telemetrySubscriber;

    @Before
    public void init() {
        configuration = null;
    }

    @Test
    public void shouldNotBeAbleToProcessWhenConfigIsNull() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(daggerContext, null, telemetrySubscriber);
        assertFalse(parentPostProcessor.canProcess(null));
    }

    @Test
    public void shouldNotBeAbleToProcessWhenConfigIsEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(daggerContext, null, telemetrySubscriber);
        PostProcessorConfig mockConfig = mock(PostProcessorConfig.class);
        when(mockConfig.isEmpty()).thenReturn(true);
        assertFalse(parentPostProcessor.canProcess(mockConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(daggerContext, null, telemetrySubscriber);
        PostProcessorConfig mockConfig = mock(PostProcessorConfig.class);
        when(mockConfig.isEmpty()).thenReturn(false);
        assertTrue(parentPostProcessor.canProcess(mockConfig));
    }


}
