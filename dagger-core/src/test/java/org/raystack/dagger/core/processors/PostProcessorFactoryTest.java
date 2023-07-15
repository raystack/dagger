package org.raystack.dagger.core.processors;

import org.raystack.dagger.common.core.DaggerContextTestBase;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.core.processors.types.PostProcessor;
import org.raystack.dagger.core.utils.Constants;
import org.raystack.dagger.core.processors.longbow.LongbowProcessor;
import org.raystack.dagger.core.processors.telemetry.TelemetryProcessor;
import org.raystack.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

import static org.raystack.dagger.common.core.Constants.INPUT_STREAMS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class PostProcessorFactoryTest extends DaggerContextTestBase {

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    private String[] columnNames;

    private String jsonArray = "[\n"
            + "        {\n"
            + "            \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"4\",\n"
            + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\",\n"
            + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\",\n"
            + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:6667\",\n"
            + "            \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"flink-sql-flud-gp0330\",\n"
            + "            \"INPUT_SCHEMA_PROTO_CLASS\": \"TestLogMessage\",\n"
            + "            \"INPUT_SCHEMA_TABLE\": \"data_stream\",\n"
            + "            \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\"\n"
            + "        }\n"
            + "]";

    @Before
    public void setup() {
        initMocks(this);
        columnNames = new String[]{"a", "b", "longbow_duration"};
    }


    @Test
    public void shouldReturnLongbowProcessor() {
        columnNames = new String[]{"longbow_key", "longbow_data", "event_timestamp", "rowtime", "longbow_duration"};
        when(configuration.getString(Constants.FLINK_SQL_QUERY_KEY, Constants.FLINK_SQL_QUERY_DEFAULT)).thenReturn("select a as `longbow_key` from l");
        when(configuration.getBoolean(Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_POSTPROCESSOR_ENABLE_DEFAULT)).thenReturn(false);
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn(jsonArray);

        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(daggerContext, stencilClientOrchestrator, columnNames, metricsTelemetryExporter);

        assertEquals(1, postProcessors.size());
        assertEquals(LongbowProcessor.class, postProcessors.get(0).getClass());
    }

    @Test
    public void shouldReturnParentPostProcessor() {
        when(configuration.getString(Constants.FLINK_SQL_QUERY_KEY, Constants.FLINK_SQL_QUERY_DEFAULT)).thenReturn("test-sql");
        when(configuration.getBoolean(Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_POSTPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);

        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(daggerContext, stencilClientOrchestrator, columnNames, metricsTelemetryExporter);

        assertEquals(1, postProcessors.size());
        assertEquals(ParentPostProcessor.class, postProcessors.get(0).getClass());
    }

    @Test
    public void shouldReturnTelemetryPostProcessor() {
        when(configuration.getString(Constants.FLINK_SQL_QUERY_KEY, Constants.FLINK_SQL_QUERY_DEFAULT)).thenReturn("test-sql");
        when(configuration.getBoolean(Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_POSTPROCESSOR_ENABLE_DEFAULT)).thenReturn(false);
        when(configuration.getBoolean(Constants.METRIC_TELEMETRY_ENABLE_KEY, Constants.METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT)).thenReturn(true);

        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(daggerContext, stencilClientOrchestrator, columnNames, metricsTelemetryExporter);

        assertEquals(1, postProcessors.size());
        assertEquals(TelemetryProcessor.class, postProcessors.get(0).getClass());
    }

    @Test
    public void shouldNotReturnAnyPostProcessor() {
        when(configuration.getString(Constants.FLINK_SQL_QUERY_KEY, Constants.FLINK_SQL_QUERY_DEFAULT)).thenReturn("test-sql");
        when(configuration.getBoolean(Constants.PROCESSOR_POSTPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_POSTPROCESSOR_ENABLE_DEFAULT)).thenReturn(false);
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(daggerContext, stencilClientOrchestrator, columnNames, metricsTelemetryExporter);

        assertEquals(0, postProcessors.size());
    }
}
