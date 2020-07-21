package com.gojek.daggers.sink;

import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import com.gojek.daggers.metrics.reporters.NoOpErrorReporter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.gojek.daggers.utils.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FlinkKafkaProducer010CustomTest {

    @Mock
    private FlinkKafkaProducer<Row> flinkKafkaProducer;

    @Mock
    private FunctionSnapshotContext functionSnapshotContext;

    @Mock
    private FunctionInitializationContext functionInitializationContext;

    @Mock
    private Configuration configuration;

    @Mock
    private Context context;

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private ErrorReporter errorStatsReporter;

    @Mock
    private NoOpErrorReporter noOpErrorReporter;

    private FlinkKafkaProducerCustomStub flinkKafkaProducerCustomStub;
    private Row row;

    @Before
    public void setUp() {
        initMocks(this);
        flinkKafkaProducerCustomStub = new FlinkKafkaProducerCustomStub(flinkKafkaProducer, configuration);
        row = new Row(1);
        row.setField(0, "some field");
    }

    @Test
    public void shouldCallFlinkProducerOpenMethodOnOpen() throws Exception {
        FlinkKafkaProducer010Custom flinkKafkaProducer010Custom = new FlinkKafkaProducer010Custom(flinkKafkaProducer, configuration);
        flinkKafkaProducer010Custom.open(configuration);

        verify(flinkKafkaProducer, times(1)).open(configuration);
    }

    @Test
    public void shouldCallFlinkProducerCloseMethodOnClose() throws Exception {
        FlinkKafkaProducer010Custom flinkKafkaProducer010Custom = new FlinkKafkaProducer010Custom(flinkKafkaProducer, configuration);
        flinkKafkaProducer010Custom.close();

        verify(flinkKafkaProducer, times(1)).close();
    }

    @Test
    public void shouldCallFlinkProducerSnapshotState() throws Exception {
        FlinkKafkaProducer010Custom flinkKafkaProducer010Custom = new FlinkKafkaProducer010Custom(flinkKafkaProducer, configuration);
        flinkKafkaProducer010Custom.snapshotState(functionSnapshotContext);

        verify(flinkKafkaProducer, times(1)).snapshotState(functionSnapshotContext);
    }

    @Test
    public void shouldCallFlinkProducerInitializeState() throws Exception {
        FlinkKafkaProducer010Custom flinkKafkaProducer010Custom = new FlinkKafkaProducer010Custom(flinkKafkaProducer, configuration);
        flinkKafkaProducer010Custom.initializeState(functionInitializationContext);

        verify(flinkKafkaProducer, times(1)).initializeState(functionInitializationContext);
    }

    @Test
    public void shouldCallFlinkProducerGetIterationRuntimeContext() {
        FlinkKafkaProducer010Custom flinkKafkaProducer010Custom = new FlinkKafkaProducer010Custom(flinkKafkaProducer, configuration);
        flinkKafkaProducer010Custom.getIterationRuntimeContext();

        verify(flinkKafkaProducer, times(1)).getIterationRuntimeContext();
    }

    @Test
    public void shouldCallFlinkProducerGetRuntimeContext() {
        FlinkKafkaProducer010Custom flinkKafkaProducer010Custom = new FlinkKafkaProducer010Custom(flinkKafkaProducer, configuration);
        flinkKafkaProducer010Custom.getRuntimeContext();

        verify(flinkKafkaProducer, times(1)).getRuntimeContext();
    }

    @Test
    public void shouldCallFlinkProducerSetRuntimeContext() {
        FlinkKafkaProducer010Custom flinkKafkaProducer010Custom = new FlinkKafkaProducer010Custom(flinkKafkaProducer, configuration);
        flinkKafkaProducer010Custom.setRuntimeContext(runtimeContext);

        verify(flinkKafkaProducer, times(1)).setRuntimeContext(runtimeContext);
    }


    @Test
    public void shouldReportErrorIfTelemetryEnabled() {
        when(configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)).thenReturn(true);
        when(configuration.getLong(SHUTDOWN_PERIOD_KEY, SHUTDOWN_PERIOD_DEFAULT)).thenReturn(0L);

        try {
            flinkKafkaProducerCustomStub.invoke(row, context);
        } catch (Exception e) {
            Assert.assertEquals("test producer exception", e.getMessage());
        }
        verify(errorStatsReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldNotReportIfTelemetryDisabled() {
        when(configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)).thenReturn(false);

        try {
            flinkKafkaProducerCustomStub.invoke(row, context);
        } catch (Exception e) {
            Assert.assertEquals("test producer exception", e.getMessage());
        }
        verify(noOpErrorReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldReturnErrorStatsReporter() {
        when(configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)).thenReturn(true);
        ErrorReporter expectedErrorStatsReporter = ErrorReporterFactory.getErrorReporter(runtimeContext, configuration);
        FlinkKafkaProducer010Custom flinkKafkaProducer010Custom = new FlinkKafkaProducer010Custom(flinkKafkaProducer, configuration);
        Assert.assertEquals(expectedErrorStatsReporter.getClass(), flinkKafkaProducer010Custom.getErrorReporter(runtimeContext).getClass());
    }

    public class FlinkKafkaProducerCustomStub extends FlinkKafkaProducer010Custom {
        FlinkKafkaProducerCustomStub(FlinkKafkaProducer<Row> flinkKafkaProducer, Configuration configuration) {
            super(flinkKafkaProducer, configuration);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }

        protected ErrorReporter getErrorReporter(RuntimeContext runtimeContext) {
            if (configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT)) {
                return errorStatsReporter;
            } else return noOpErrorReporter;
        }

        protected void invokeBaseProducer(Row value, Context context) {
            throw new RuntimeException("test producer exception");
        }
    }
}