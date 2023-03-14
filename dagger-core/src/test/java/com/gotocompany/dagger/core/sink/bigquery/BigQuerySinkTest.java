package com.gotocompany.dagger.core.sink.bigquery;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.serde.proto.serialization.ProtoSerializer;
import com.gotocompany.dagger.core.metrics.reporters.statsd.DaggerStatsDReporter;
import com.gotocompany.depot.bigquery.BigQuerySinkFactory;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BigQuerySinkTest {

    @Test
    public void shouldReturnCommittersAndSerializer() throws IOException {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        BigQuerySinkFactory sinkFactory = Mockito.mock(BigQuerySinkFactory.class);
        DaggerStatsDReporter daggerStatsDReporter = Mockito.mock(DaggerStatsDReporter.class);
        Configuration configuration = new Configuration(ParameterTool.fromMap(Collections.emptyMap()));
        BigQuerySink sink = new BigQuerySink(configuration, protoSerializer, sinkFactory, daggerStatsDReporter);
        Assert.assertEquals(Optional.empty(), sink.createCommitter());
        Assert.assertEquals(Optional.empty(), sink.getWriterStateSerializer());
        Assert.assertEquals(Optional.empty(), sink.createGlobalCommitter());
        Assert.assertEquals(Optional.empty(), sink.getCommittableSerializer());
        Assert.assertEquals(Optional.empty(), sink.getGlobalCommittableSerializer());
    }

    @Test
    public void shouldCreateSinkWriter() {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        BigQuerySinkFactory sinkFactory = Mockito.mock(BigQuerySinkFactory.class);
        DaggerStatsDReporter daggerStatsDReporter = Mockito.mock(DaggerStatsDReporter.class);
        Sink.InitContext context = Mockito.mock(Sink.InitContext.class);
        SinkWriterMetricGroup metricGroup = Mockito.mock(SinkWriterMetricGroup.class);
        Mockito.when(context.metricGroup()).thenReturn(metricGroup);
        com.gotocompany.depot.Sink mockSink = Mockito.mock(com.gotocompany.depot.Sink.class);
        Map<String, String> configMap = new HashMap<>();
        Configuration configuration = new Configuration(ParameterTool.fromMap(configMap));
        Mockito.when(sinkFactory.create()).thenReturn(mockSink);
        BigQuerySink sink = new BigQuerySink(configuration, protoSerializer, sinkFactory, daggerStatsDReporter);
        SinkWriter<Row, Void, Void> writer = sink.createWriter(context, null);
        Assert.assertTrue(writer instanceof BigQuerySinkWriter);
        Mockito.verify(sinkFactory, Mockito.times(1)).create();
    }
}
