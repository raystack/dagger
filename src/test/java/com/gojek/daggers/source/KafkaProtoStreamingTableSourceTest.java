package com.gojek.daggers.source;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
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
    private SingleOutputStreamOperator dataStream;

    @Mock
    private DataStreamSource dataStreamSource;

    @Mock
    private FlinkKafkaConsumerBase flinkConsumer;

    @Mock
    private ProtoType protoType;

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
    public void shouldGiveBackRowTypesComingFromProtoType() {
        RowTypeInfo expectedRowType = new RowTypeInfo();
        when(flinkConsumer.getProducedType()).thenReturn(expectedRowType);
        KafkaProtoStreamingTableSource kafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(flinkConsumer, "", 0L, true);

        assertEquals(expectedRowType, kafkaProtoStreamingTableSource.getReturnType());
    }
}
