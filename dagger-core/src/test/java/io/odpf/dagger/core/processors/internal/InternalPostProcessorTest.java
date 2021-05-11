package io.odpf.dagger.core.processors.internal;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.PostProcessorConfig;
import io.odpf.dagger.core.processors.external.ExternalSourceConfig;
import io.odpf.dagger.core.processors.transformers.TransformConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalPostProcessorTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void canProcessWhenInternalConfigIsPresent() {
        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        ArrayList<TransformConfig> transformers = new ArrayList<>();
        ArrayList<InternalSourceConfig> internalSourceConfigs = new ArrayList<>();
        internalSourceConfigs.add(new InternalSourceConfig("output_field", "value", "sql"));
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, transformers, internalSourceConfigs);
        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(postProcessorConfig);

        Assert.assertTrue(internalPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void canNotProcessWhenInternalConfigIsNull() {
        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        ArrayList<TransformConfig> transformers = new ArrayList<>();
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, transformers, null);
        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(postProcessorConfig);

        Assert.assertFalse(internalPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldNotBeAbleToProcessWhenOutputFieldIsMissing() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [output_field]");

        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        ArrayList<TransformConfig> transformers = new ArrayList<>();
        ArrayList<InternalSourceConfig> internalSourceConfigs = new ArrayList<>();
        internalSourceConfigs.add(new InternalSourceConfig("", "value", "sql"));
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, transformers, internalSourceConfigs);
        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(postProcessorConfig);
        StreamInfo streamInfoMock = mock(StreamInfo.class);
        when(streamInfoMock.getColumnNames()).thenReturn(new String[]{"order_id", "customer_id"});

        internalPostProcessor.process(streamInfoMock);
    }

    @Test
    public void shouldNotBeAbleToProcessWhenValueIsMissing() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [value]");

        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        ArrayList<TransformConfig> transformers = new ArrayList<>();
        ArrayList<InternalSourceConfig> internalSourceConfigs = new ArrayList<>();
        internalSourceConfigs.add(new InternalSourceConfig("output", "", "sql"));
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, transformers, internalSourceConfigs);
        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(postProcessorConfig);
        StreamInfo streamInfoMock = mock(StreamInfo.class);
        when(streamInfoMock.getColumnNames()).thenReturn(new String[]{"order_id", "customer_id"});

        internalPostProcessor.process(streamInfoMock);
    }

    @Test
    public void processWithRightConfiguration() {
        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        ArrayList<TransformConfig> transformers = new ArrayList<>();
        ArrayList<InternalSourceConfig> internalSourceConfigs = new ArrayList<>();

        internalSourceConfigs.add(new InternalSourceConfig("output", "order_id", "sql"));

        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, transformers, internalSourceConfigs);

        InternalPostProcessor internalPostProcessor = new InternalPostProcessor(postProcessorConfig);

        StreamInfo streamInfoMock = mock(StreamInfo.class);
        DataStream resultStream = mock(DataStream.class);
        when(streamInfoMock.getColumnNames()).thenReturn(new String[]{"order_id", "customer_id"});
        when(streamInfoMock.getDataStream()).thenReturn(resultStream);

        internalPostProcessor.process(streamInfoMock);
    }
}
