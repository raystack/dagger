package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.gojek.daggers.utils.Constants.INPUT_STREAMS;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowProcessorFactoryTest {

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private Configuration configuration;

    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnLongbowWriteProcessorWhenLongbowSchemaContainsLongbowWrite() {
        String[] columnNames = new String[]{"longbow_write_key", "longbow_write", "rowtime"};
        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[{\"PROTO_CLASS_NAME\": \"com.gojek.esb.booking.BookingLogMessage\"}]");
        LongbowProcessorFactory longbowProcessorFactory = new LongbowProcessorFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter);
        PostProcessor longbowProcessor = longbowProcessorFactory.getLongbowProcessor();
        Assert.assertEquals(LongbowWriteProcessor.class, longbowProcessor.getClass());
    }

    @Test
    public void shouldReturnLongbowReadProcessorWhenLongbowSchemaContainsLongbowRead() {
        String[] columnNames = new String[]{"longbow_read_key", "rowtime", "longbow_duration", "proto_data", "event_timestamp"};
        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        LongbowProcessorFactory longbowProcessorFactory = new LongbowProcessorFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter);
        PostProcessor longbowProcessor = longbowProcessorFactory.getLongbowProcessor();
        Assert.assertEquals(LongbowReadProcessor.class, longbowProcessor.getClass());
    }

    @Test
    public void shouldReturnLongbowProcessorWhenLongbowSchemaDoesNotContainLongbowReadOrLongbowWrite() {
        String[] columnNames = new String[]{"longbow_key", "longbow_data", "rowtime", "event_timestamp", "longbow_duration"};
        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        LongbowProcessorFactory longbowProcessorFactory = new LongbowProcessorFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter);
        PostProcessor longbowProcessor = longbowProcessorFactory.getLongbowProcessor();
        Assert.assertEquals(LongbowProcessor.class, longbowProcessor.getClass());
    }
}
