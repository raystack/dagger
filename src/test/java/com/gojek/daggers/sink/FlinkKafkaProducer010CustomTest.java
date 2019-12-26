package com.gojek.daggers.sink;

import com.gojek.daggers.metrics.ErrorStatsReporter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.annotation.Nullable;
import java.util.Properties;

import static com.gojek.daggers.utils.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FlinkKafkaProducer010CustomTest {

    @Mock
    private KeyedSerializationSchema keyedSerializationSchema;

    @Mock
    private Properties properties;

    @Mock
    private FlinkKafkaPartitioner flinkKafkaPartitioner;

    @Mock
    private Configuration configuration;

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private ErrorStatsReporter errorStatsReporter;

    @Mock
    private SinkFunction.Context context;

    private FlinkKafkaProducerCustomStub<Row> flinkKafkaProducerCustomStub;
    private Row row;

    @Before
    public void setUp() {
        initMocks(this);
        when(properties.containsKey(any(String.class))).thenReturn(true);
        flinkKafkaProducerCustomStub = new FlinkKafkaProducerCustomStub<>("test_topic", keyedSerializationSchema, properties, flinkKafkaPartitioner, configuration);
        row = new Row(1);
        row.setField(0, "some field");
    }


    @org.junit.Test
    public void shouldReportIfTelemetryEnabled() {
        when(configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)).thenReturn(true);
        when(configuration.getLong(SHUTDOWN_PERIOD_KEY, SHUTDOWN_PERIOD_DEFAULT)).thenReturn(0L);

        try {
            flinkKafkaProducerCustomStub.invoke(row, context);
        } catch (Exception e) {
            Assert.assertEquals("test producer exception", e.getMessage());
        }
        verify(errorStatsReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @org.junit.Test
    public void shouldNotReportIfTelemetryDisabled() {
        when(configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)).thenReturn(false);

        try {
            flinkKafkaProducerCustomStub.invoke(row, context);
        } catch (Exception e) {
            Assert.assertEquals("test producer exception", e.getMessage());
        }
        verify(errorStatsReporter, times(0)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldReturnErrorStatsReporter() {
        ErrorStatsReporter expectedErrorStatsReporter = new ErrorStatsReporter(runtimeContext, configuration);
        Assert.assertEquals(expectedErrorStatsReporter.getClass(), flinkKafkaProducerCustomStub.getErrorStatsReporter().getClass());
    }

    public class FlinkKafkaProducerCustomStub<T> extends FlinkKafkaProducer010Custom<T> {
        public FlinkKafkaProducerCustomStub(String topicId, KeyedSerializationSchema serializationSchema,
                                            Properties producerConfig, @Nullable FlinkKafkaPartitioner customPartitioner, Configuration configuration) {
            super(topicId, serializationSchema, producerConfig, customPartitioner, configuration);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }

        protected ErrorStatsReporter getErrorStatsReporter(RuntimeContext runtimeContext) {
            return errorStatsReporter;
        }

        protected ErrorStatsReporter getErrorStatsReporter() {
            return super.getErrorStatsReporter(runtimeContext);
        }

        protected void invokeBaseProducer(T value, Context context) {
            throw new RuntimeException("test producer exception");
        }
    }
}