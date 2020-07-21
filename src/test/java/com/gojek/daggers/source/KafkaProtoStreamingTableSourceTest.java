package com.gojek.daggers.source;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class KafkaProtoStreamingTableSourceTest {

    @Mock
    private StreamExecutionEnvironment streamEnv;

    @Mock
    private DataStreamSource dataStreamSource;

    @Mock
    private FlinkKafkaConsumer011Custom flinkConsumer;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldUseInjectedNameForRowtimeAttribute() {
        String expectedRowTimeAttribute = "window_timestamp";
        assertEquals(expectedRowTimeAttribute, new KafkaProtoStreamingTableSource(null, "window_timestamp", 0L, true).getRowtimeAttributeDescriptors().get(0).getAttributeName());
    }

    @Test
    public void shouldReturnPreservedWatermarkStrategyWhenPerPartitionWatermarkingEnabled() {
        assertEquals(PreserveWatermarks.class, new KafkaProtoStreamingTableSource(null, "window_timestamp", 0L, true)
                .getRowtimeAttributeDescriptors().get(0).getWatermarkStrategy().getClass());
    }

    @Test
    public void shouldReturnBoundedOutOfOrderTimestampsWatermarkStrategyWhenPerPartitionWatermarkingDisabled() {
        assertEquals(BoundedOutOfOrderTimestamps.class, new KafkaProtoStreamingTableSource(null, "window_timestamp", 0L, false)
                .getRowtimeAttributeDescriptors().get(0).getWatermarkStrategy().getClass());
    }

    @Test
    public void shouldGiveBackRowTypesComingFromProtoType() {
        RowTypeInfo expectedRowType = new RowTypeInfo();
        when(flinkConsumer.getProducedType()).thenReturn(expectedRowType);
        KafkaProtoStreamingTableSource kafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(flinkConsumer, "", 0L, true);

        assertEquals(expectedRowType, kafkaProtoStreamingTableSource.getReturnType());
    }

    @Test
    public void shouldReturnTableSchema() {
        RowTypeInfo rowType = new RowTypeInfo();
        when(flinkConsumer.getProducedType()).thenReturn(rowType);
        KafkaProtoStreamingTableSource kafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(flinkConsumer, "", 0L, false);
        assertEquals(TableSchema.fromTypeInfo(rowType), kafkaProtoStreamingTableSource.getTableSchema());
    }

    @Test
    public void shouldExplainSource() {
        KafkaProtoStreamingTableSource kafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(flinkConsumer, "", 0L, false);
        assertEquals("Lets you write sql queries for streaming protobuf data from kafka!!", kafkaProtoStreamingTableSource.explainSource());
    }

    @Test
    public void shouldReturnDataStream() {
        when(streamEnv.addSource(flinkConsumer)).thenReturn(dataStreamSource);
        KafkaProtoStreamingTableSource kafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(flinkConsumer, "", 0L, false);
        assertEquals(dataStreamSource, kafkaProtoStreamingTableSource.getDataStream(streamEnv));
    }
}
