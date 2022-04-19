package io.odpf.dagger.core.source.kafka;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import io.odpf.dagger.core.metrics.reporters.NoOpErrorReporter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Properties;
import java.util.regex.Pattern;

import static io.odpf.dagger.core.utils.Constants.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FlinkKafkaConsumerCustomTest {

    @Mock
    private SourceFunction.SourceContext defaultSourceContext;

    @Mock
    private Configuration configuration;

    @Mock
    private KafkaDeserializationSchema kafkaDeserializationSchema;

    @Mock
    private RuntimeContext defaultRuntimeContext;

    @Mock
    private Properties properties;

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private NoOpErrorReporter noOpErrorReporter;

    private FlinkKafkaConsumerCustomStub flinkKafkaConsumer011Custom;

    @Before
    public void setup() {
        initMocks(this);
        flinkKafkaConsumer011Custom = new FlinkKafkaConsumerCustomStub(Pattern.compile("test_topics"), kafkaDeserializationSchema, properties, configuration, new RuntimeException("test exception"));
    }

    @Test
    public void shouldReportIfTelemetryEnabled() {
        when(configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(true);
        when(configuration.getLong(METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_KEY, METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT)).thenReturn(0L);

        Exception exception = Assert.assertThrows(Exception.class,
                () -> flinkKafkaConsumer011Custom.run(defaultSourceContext));
        assertEquals("test exception", exception.getMessage());
        verify(errorReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldNotReportIfChainedOperatorException() {
        when(configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(true);
        Throwable throwable = new Throwable();
        flinkKafkaConsumer011Custom = new FlinkKafkaConsumerCustomStub(Pattern.compile("test_topics"), kafkaDeserializationSchema, properties, configuration, new ExceptionInChainedOperatorException("chaining exception", throwable));
        Exception exception = Assert.assertThrows(Exception.class,
                () -> flinkKafkaConsumer011Custom.run(defaultSourceContext));
        assertEquals("chaining exception", exception.getMessage());
        verify(errorReporter, times(0)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldNotReportIfTelemetryDisabled() {
        when(configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(false);
        Exception exception = Assert.assertThrows(Exception.class,
                () -> flinkKafkaConsumer011Custom.run(defaultSourceContext));
        assertEquals("test exception", exception.getMessage());
        verify(noOpErrorReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldReturnErrorStatsReporter() {
        when(configuration.getLong(METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_KEY, METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT)).thenReturn(0L);
        when(configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(true);
        ErrorReporter expectedErrorStatsReporter = ErrorReporterFactory.getErrorReporter(defaultRuntimeContext.getMetricGroup(), configuration);
        FlinkKafkaConsumerCustom flinkKafkaConsumerCustom = new FlinkKafkaConsumerCustom(Pattern.compile("test_topics"), kafkaDeserializationSchema, properties, configuration);
        assertEquals(expectedErrorStatsReporter.getClass(), flinkKafkaConsumerCustom.getErrorReporter(defaultRuntimeContext).getClass());
    }

    public class FlinkKafkaConsumerCustomStub extends FlinkKafkaConsumerCustom {
        private Exception exception;

        public FlinkKafkaConsumerCustomStub(Pattern subscriptionPattern, KafkaDeserializationSchema deserializer,
                                            Properties props, Configuration configuration, Exception exception) {
            super(subscriptionPattern, deserializer, props, configuration);
            this.exception = exception;
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return defaultRuntimeContext;
        }

        protected void runBaseConsumer(SourceContext sourceContext) throws Exception {
            throw exception;
        }

        protected ErrorReporter getErrorReporter(RuntimeContext runtimeContext) {
            if (configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)) {
                return errorReporter;
            } else {
                return noOpErrorReporter;
            }
        }
    }
}
