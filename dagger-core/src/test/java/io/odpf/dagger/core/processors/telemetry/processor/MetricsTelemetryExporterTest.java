package io.odpf.dagger.core.processors.telemetry.processor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.metrics.managers.GaugeStatsManager;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class MetricsTelemetryExporterTest {

    private MetricsTelemetryExporter metricsTelemetryExporter;

    @Mock
    private GaugeStatsManager gaugeStatsManager;

    @Mock
    private Configuration configuration;

    @Mock
    private RuntimeContext runtimeContext;

    @Captor
    private ArgumentCaptor<String> keyCaptor;

    @Captor
    private ArgumentCaptor<String> valueCaptor;

    @Mock
    private TelemetryPublisher topicPublisher;

    @Mock
    private TelemetryPublisher sinkPublisher;

    @Mock
    private OperatorMetricGroup metricGroup;

    @Mock
    private Gauge gauge;


    @Before
    public void setUp() {
        initMocks(this);
        metricsTelemetryExporter = new MetricsTelemetryExporter(gaugeStatsManager);
    }

    @Test
    public void shouldReturnSameInputRowOnMap() throws Exception {
        Row inputRow = Row.of("test_value");
        Row outputRow = metricsTelemetryExporter.map(inputRow);
        assertEquals(Row.of("test_value"), outputRow);
    }

    @Test
    public void shouldRegisterGroupsOnUpdateWithSingleKey() {
        ArrayList<String> topicNames = new ArrayList<>();
        topicNames.add("topic1");
        topicNames.add("topic2");
        topicNames.add("topic3");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("topic", topicNames);

        when(topicPublisher.getTelemetry()).thenReturn(metrics);

        metricsTelemetryExporter.updated(topicPublisher);
        verify(gaugeStatsManager, times(3)).registerAspects(keyCaptor.capture(), valueCaptor.capture(), any(), any(Integer.class));

        List<String> allKeys = keyCaptor.getAllValues();
        List<String> allValues = valueCaptor.getAllValues();

        assertEquals(asList("topic", "topic", "topic"), allKeys);
        assertEquals(asList("topic1", "topic2", "topic3"), allValues);
    }


    @Test
    public void shouldRegisterGroupsOnUpdateWithMultipleKeys() {
        ArrayList<String> topicNames = new ArrayList<>();
        topicNames.add("topic1");
        topicNames.add("topic2");
        topicNames.add("topic3");
        HashMap<String, List<String>> topicMetric = new HashMap<>();
        topicMetric.put("topic", topicNames);

        ArrayList<String> sink = new ArrayList<>();
        sink.add("log");

        HashMap<String, List<String>> sinkMetric = new HashMap<>();
        sinkMetric.put("sink", sink);

        when(topicPublisher.getTelemetry()).thenReturn(topicMetric);
        when(sinkPublisher.getTelemetry()).thenReturn(sinkMetric);

        metricsTelemetryExporter.updated(topicPublisher);
        metricsTelemetryExporter.updated(sinkPublisher);

        verify(gaugeStatsManager, times(4)).registerAspects(keyCaptor.capture(), valueCaptor.capture(), any(), any(Integer.class));

        List<String> allKeys = keyCaptor.getAllValues();
        List<String> allValues = valueCaptor.getAllValues();

        assertEquals(asList("topic", "topic", "topic", "sink"), allKeys);
        assertEquals(asList("topic1", "topic2", "topic3", "log"), allValues);
    }

    @Test
    public void shouldRegisterGroupsOnUpdateWithSameKeysWithDifferentValues() throws Exception {
        ArrayList<String> topicNames1 = new ArrayList<>();
        topicNames1.add("topic1");
        HashMap<String, List<String>> topicMetric1 = new HashMap<>();
        topicMetric1.put("topic", topicNames1);

        ArrayList<String> topicNames2 = new ArrayList<>();
        topicNames2.add("topic2");
        HashMap<String, List<String>> topicMetric2 = new HashMap<>();
        topicMetric2.put("topic", topicNames2);

        when(topicPublisher.getTelemetry()).thenReturn(topicMetric1).thenReturn(topicMetric2);

        MetricsTelemetryExporterStub metricsTelemetryExporterStub = new MetricsTelemetryExporterStub();
        metricsTelemetryExporterStub.updated(topicPublisher);
        metricsTelemetryExporterStub.updated(topicPublisher);

        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(any(), any())).thenReturn(metricGroup);

        when(metricGroup.gauge(any(String.class), any(Gauge.class))).thenReturn(gauge);
        metricsTelemetryExporterStub.open(configuration);

        verify(metricGroup, times(2)).addGroup(keyCaptor.capture(), valueCaptor.capture());

        List<String> allKeys = keyCaptor.getAllValues();
        List<String> allValues = valueCaptor.getAllValues();

        assertEquals(asList("topic", "topic"), allKeys);
        assertEquals(asList("topic1", "topic2"), allValues);
    }

    @Test
    public void shouldRegisterGroupsOnOpen() throws Exception {
        ArrayList<String> topicNames = new ArrayList<>();
        topicNames.add("topic1");
        topicNames.add("topic2");
        topicNames.add("topic3");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("topic", topicNames);

        when(topicPublisher.getTelemetry()).thenReturn(metrics);

        MetricsTelemetryExporterStub metricsTelemetryExporterStub = new MetricsTelemetryExporterStub();
        metricsTelemetryExporterStub.updated(topicPublisher);

        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(any(), any())).thenReturn(metricGroup);

        when(metricGroup.gauge(any(String.class), any(Gauge.class))).thenReturn(gauge);
        metricsTelemetryExporterStub.open(configuration);

        verify(metricGroup, times(3)).addGroup(keyCaptor.capture(), valueCaptor.capture());

        List<String> allKeys = keyCaptor.getAllValues();
        List<String> allValues = valueCaptor.getAllValues();

        assertEquals(asList("topic", "topic", "topic"), allKeys);
        assertEquals(asList("topic1", "topic2", "topic3"), allValues);
    }

    public class MetricsTelemetryExporterStub extends MetricsTelemetryExporter {
        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }
    }
}
