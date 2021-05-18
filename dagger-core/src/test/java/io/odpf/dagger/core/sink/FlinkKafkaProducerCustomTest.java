package io.odpf.dagger.core.sink;

import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import io.odpf.dagger.core.metrics.reporters.NoOpErrorReporter;
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

import static io.odpf.dagger.core.utils.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class FlinkKafkaProducerCustomTest {

    @Mock
    private FlinkKafkaProducer<Row> flinkKafkaProducer;

    @Mock
    private FunctionSnapshotContext functionSnapshotContext;

    @Mock
    private FunctionInitializationContext functionInitializationContext;

    @Mock
    private Configuration configuration;

    @Mock
    private Context defaultContext;

    @Mock
    private RuntimeContext defaultRuntimeContext;

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
        FlinkKafkaProducerCustom flinkKafkaProducerCustom = new FlinkKafkaProducerCustom(flinkKafkaProducer, configuration);
        flinkKafkaProducerCustom.open(configuration);

        verify(flinkKafkaProducer, times(1)).open(configuration);
    }

    @Test
    public void shouldCallFlinkProducerCloseMethodOnClose() throws Exception {
        FlinkKafkaProducerCustom flinkKafkaProducerCustom = new FlinkKafkaProducerCustom(flinkKafkaProducer, configuration);
        flinkKafkaProducerCustom.close();

        verify(flinkKafkaProducer, times(1)).close();
    }

    @Test
    public void shouldCallFlinkProducerSnapshotState() throws Exception {
        FlinkKafkaProducerCustom flinkKafkaProducerCustom = new FlinkKafkaProducerCustom(flinkKafkaProducer, configuration);
        flinkKafkaProducerCustom.snapshotState(functionSnapshotContext);

        verify(flinkKafkaProducer, times(1)).snapshotState(functionSnapshotContext);
    }

    @Test
    public void shouldCallFlinkProducerInitializeState() throws Exception {
        FlinkKafkaProducerCustom flinkKafkaProducerCustom = new FlinkKafkaProducerCustom(flinkKafkaProducer, configuration);
        flinkKafkaProducerCustom.initializeState(functionInitializationContext);

        verify(flinkKafkaProducer, times(1)).initializeState(functionInitializationContext);
    }

    @Test
    public void shouldCallFlinkProducerGetIterationRuntimeContext() {
        FlinkKafkaProducerCustom flinkKafkaProducerCustom = new FlinkKafkaProducerCustom(flinkKafkaProducer, configuration);
        flinkKafkaProducerCustom.getIterationRuntimeContext();

        verify(flinkKafkaProducer, times(1)).getIterationRuntimeContext();
    }

    @Test
    public void shouldCallFlinkProducerGetRuntimeContext() {
        FlinkKafkaProducerCustom flinkKafkaProducerCustom = new FlinkKafkaProducerCustom(flinkKafkaProducer, configuration);
        flinkKafkaProducerCustom.getRuntimeContext();

        verify(flinkKafkaProducer, times(1)).getRuntimeContext();
    }

    @Test
    public void shouldCallFlinkProducerSetRuntimeContext() {
        FlinkKafkaProducerCustom flinkKafkaProducerCustom = new FlinkKafkaProducerCustom(flinkKafkaProducer, configuration);
        flinkKafkaProducerCustom.setRuntimeContext(defaultRuntimeContext);

        verify(flinkKafkaProducer, times(1)).setRuntimeContext(defaultRuntimeContext);
    }


    @Test
    public void shouldReportErrorIfTelemetryEnabled() {
        when(configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(true);
        when(configuration.getLong(METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_KEY, METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT)).thenReturn(0L);

        try {
            flinkKafkaProducerCustomStub.invoke(row, defaultContext);
        } catch (Exception e) {
            Assert.assertEquals("test producer exception", e.getMessage());
        }
        verify(errorStatsReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldNotReportIfTelemetryDisabled() {
        when(configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(false);

        try {
            flinkKafkaProducerCustomStub.invoke(row, defaultContext);
        } catch (Exception e) {
            Assert.assertEquals("test producer exception", e.getMessage());
        }
        verify(noOpErrorReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldReturnErrorStatsReporter() {
        when(configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(true);
        ErrorReporter expectedErrorStatsReporter = ErrorReporterFactory.getErrorReporter(defaultRuntimeContext, configuration);
        FlinkKafkaProducerCustom flinkKafkaProducerCustom = new FlinkKafkaProducerCustom(flinkKafkaProducer, configuration);
        Assert.assertEquals(expectedErrorStatsReporter.getClass(), flinkKafkaProducerCustom.getErrorReporter(defaultRuntimeContext).getClass());
    }

    public class FlinkKafkaProducerCustomStub extends FlinkKafkaProducerCustom {
        FlinkKafkaProducerCustomStub(FlinkKafkaProducer<Row> flinkKafkaProducer, Configuration configuration) {
            super(flinkKafkaProducer, configuration);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return defaultRuntimeContext;
        }

        protected ErrorReporter getErrorReporter(RuntimeContext runtimeContext) {
            if (configuration.getBoolean(METRIC_TELEMETRY_ENABLE_KEY, METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)) {
                return errorStatsReporter;
            } else {
                return noOpErrorReporter;
            }
        }

        protected void invokeBaseProducer(Row value, Context context) {
            throw new RuntimeException("test producer exception");
        }
    }
}
