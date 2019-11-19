package com.gojek.daggers.postProcessors.internal;

import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.internal.processor.InternalConfigProcessor;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class InternalDecoratorTest {

    @Test
    public void canDecorateWhenInternalConfigIsPresent() {
        InternalDecorator internalDecorator = new InternalDecorator(mock(InternalSourceConfig.class), null);

        Assert.assertTrue(internalDecorator.canDecorate());
    }

    @Test
    public void canNotDecorateWhenInternalConfigIsNotPresent() {
        InternalDecorator internalDecorator = new InternalDecorator(null, null);

        Assert.assertFalse(internalDecorator.canDecorate());
    }

    @Test
    public void shouldMapUsingProcessor() {
        InternalConfigProcessor processorMock = mock(InternalConfigProcessor.class);
        InternalDecorator internalDecorator = new InternalDecorator(null, processorMock);
        Row dataStreamRow = new Row(2);

        internalDecorator.map(dataStreamRow);

        verify(processorMock).process(new RowManager(dataStreamRow));
    }
}