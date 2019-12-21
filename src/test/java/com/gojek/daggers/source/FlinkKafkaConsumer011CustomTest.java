package com.gojek.daggers.source;

import com.gojek.daggers.metrics.ErrorStatsReporter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Properties;
import java.util.regex.Pattern;

import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_KEY;
import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_VALUE_DEFAULT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FlinkKafkaConsumer011CustomTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private SourceFunction.SourceContext sourceContext;

    @Mock
    private Configuration configuration;

    @Mock
    private KafkaDeserializationSchema kafkaDeserializationSchema;

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private Properties properties;

    @Mock
    private ErrorStatsReporter errorStatsReporter;

    private FlinkKafkaConsumerCustomStub flinkKafkaConsumer011Custom;

    @Before
    public void setup() {
        initMocks(this);
        flinkKafkaConsumer011Custom = new FlinkKafkaConsumerCustomStub(Pattern.compile("test_topics"), kafkaDeserializationSchema, properties, configuration);
    }

    @org.junit.Test
    public void shouldReportIfTelemetryEnabled() throws Exception {
        when(configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)).thenReturn(true);

        try {
            flinkKafkaConsumer011Custom.run(sourceContext);
        } catch (Exception e) {
            Assert.assertEquals("test exception", e.getMessage());
        }
        verify(errorStatsReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @org.junit.Test
    public void shouldNotReportIfTelemetryDisabled() throws Exception {
        when(configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)).thenReturn(false);

        try {
            flinkKafkaConsumer011Custom.run(sourceContext);
        } catch (Exception e) {
            Assert.assertEquals("test exception", e.getMessage());
        }
        verify(errorStatsReporter, times(0)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldReturnErrorStatsReporter() {
        ErrorStatsReporter expectedErrorStatsReporter = new ErrorStatsReporter(runtimeContext);
        Assert.assertEquals(expectedErrorStatsReporter.getClass(), flinkKafkaConsumer011Custom.getErrorStatsReporter().getClass());
    }


    public class FlinkKafkaConsumerCustomStub extends FlinkKafkaConsumer011Custom {
        public FlinkKafkaConsumerCustomStub(Pattern subscriptionPattern, KafkaDeserializationSchema deserializer,
                                            Properties props, Configuration configuration) {
            super(subscriptionPattern, deserializer, props, configuration);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }

        protected void runBaseConsumer(SourceContext sourceContext) {
            throw new RuntimeException("test exception");
        }

        protected ErrorStatsReporter getErrorStatsReporter(RuntimeContext runtimeContext) {
            return errorStatsReporter;
        }

        private ErrorStatsReporter getErrorStatsReporter() {
            return super.getErrorStatsReporter(runtimeContext);
        }
    }
}