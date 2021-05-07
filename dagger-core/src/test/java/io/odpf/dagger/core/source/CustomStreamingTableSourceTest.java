package io.odpf.dagger.core.source;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
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
public class CustomStreamingTableSourceTest {

    @Mock
    private StreamExecutionEnvironment streamEnv;

    @Mock
    private DataStreamSource dataStreamSource;

    @Mock
    private FlinkKafkaConsumerCustom flinkConsumer;

    @Mock
    private DataStream dataStream;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldUseInjectedNameForRowtimeAttribute() {
        String expectedRowTimeAttribute = "window_timestamp";
        assertEquals(expectedRowTimeAttribute, new CustomStreamingTableSource("window_timestamp", 0L, true, dataStream).getRowtimeAttributeDescriptors().get(0).getAttributeName());
    }

    @Test
    public void shouldReturnPreservedWatermarkStrategyWhenPerPartitionWatermarkingEnabled() {
        assertEquals(PreserveWatermarks.class, new CustomStreamingTableSource("window_timestamp", 0L, true, dataStream)
                .getRowtimeAttributeDescriptors().get(0).getWatermarkStrategy().getClass());
    }

    @Test
    public void shouldReturnBoundedOutOfOrderTimestampsWatermarkStrategyWhenPerPartitionWatermarkingDisabled() {
        assertEquals(BoundedOutOfOrderTimestamps.class, new CustomStreamingTableSource("window_timestamp", 0L, false, dataStream)
                .getRowtimeAttributeDescriptors().get(0).getWatermarkStrategy().getClass());
    }

    @Test
    public void shouldGiveBackRowTypesComingFromProtoType() {
        RowTypeInfo expectedRowType = new RowTypeInfo();
        when(dataStream.getType()).thenReturn(expectedRowType);
        CustomStreamingTableSource customStreamingTableSource = new CustomStreamingTableSource("", 0L, true, dataStream);

        assertEquals(expectedRowType, customStreamingTableSource.getReturnType());
    }

    @Test
    public void shouldReturnTableSchema() {
        RowTypeInfo rowType = new RowTypeInfo();
        when(dataStream.getType()).thenReturn(rowType);
        CustomStreamingTableSource customStreamingTableSource = new CustomStreamingTableSource("", 0L, false, dataStream);
        assertEquals(TableSchema.fromTypeInfo(rowType), customStreamingTableSource.getTableSchema());
    }

    @Test
    public void shouldExplainSource() {
        CustomStreamingTableSource customStreamingTableSource = new CustomStreamingTableSource("", 0L, false, dataStream);
        assertEquals("Lets you write sql queries for streaming protobuf data from kafka!!", customStreamingTableSource.explainSource());
    }

    @Test
    public void shouldReturnDataStream() {
        CustomStreamingTableSource customStreamingTableSource = new CustomStreamingTableSource("", 0L, false, dataStream);
        assertEquals(dataStream, customStreamingTableSource.getDataStream(streamEnv));
    }
}
