package io.odpf.dagger.core.processors;

import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParentPostProcessorTest {

    @Mock
    private TelemetrySubscriber telemetrySubscriber;

    @Test
    public void shouldNotBeAbleToProcessWhenConfigIsNull() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);
        assertFalse(parentPostProcessor.canProcess(null));
    }

    @Test
    public void shouldNotBeAbleToProcessWhenConfigIsEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);
        PostProcessorConfig mockConfig = mock(PostProcessorConfig.class);
        when(mockConfig.isEmpty()).thenReturn(true);
        assertFalse(parentPostProcessor.canProcess(mockConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);
        PostProcessorConfig mockConfig = mock(PostProcessorConfig.class);
        when(mockConfig.isEmpty()).thenReturn(false);
        assertTrue(parentPostProcessor.canProcess(mockConfig));
    }


}
