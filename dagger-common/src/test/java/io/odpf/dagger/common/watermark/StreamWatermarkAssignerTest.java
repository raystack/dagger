package io.odpf.dagger.common.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamWatermarkAssignerTest {

    @Mock
    private DataStream<Row> inputStream;


    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAssignTimestampAndWatermarksToInputStreamIfEnablePerPartitionWatermarkNotMentioned() {
        LastColumnWatermark lastColumnWatermark = new LastColumnWatermark();
        StreamWatermarkAssigner streamWatermarkAssigner = new StreamWatermarkAssigner(lastColumnWatermark);
        streamWatermarkAssigner.assignTimeStampAndWatermark(inputStream, 10L);

        verify(inputStream, times(1)).assignTimestampsAndWatermarks(any(WatermarkStrategy.class));
    }

    @Test
    public void shouldAssignTimestampAndWatermarksToSource() {
        LastColumnWatermark lastColumnWatermark = new LastColumnWatermark();
        StreamWatermarkAssigner streamWatermarkAssigner = new StreamWatermarkAssigner(lastColumnWatermark);
        streamWatermarkAssigner.assignTimeStampAndWatermark(inputStream, 10L, false);

        verify(inputStream, times(1)).assignTimestampsAndWatermarks(any(WatermarkStrategy.class));
    }

    @Test
    public void shouldNotAssignTimestampAndWatermarksToSourceIfPerPartitionWatermarkEnabled() {
        LastColumnWatermark lastColumnWatermark = new LastColumnWatermark();
        StreamWatermarkAssigner streamWatermarkAssigner = new StreamWatermarkAssigner(lastColumnWatermark);
        streamWatermarkAssigner.assignTimeStampAndWatermark(inputStream, 10L, true);

        verify(inputStream, times(0)).assignTimestampsAndWatermarks(any(WatermarkStrategy.class));
    }

    @Test
    public void shouldAssignTimestampAndWatermarksForRowTimeStrategy() {
        String[] columnNames = {"test_field", "rowtime"};
        RowtimeFieldWatermark rowtimeFieldWatermark = new RowtimeFieldWatermark(columnNames);
        StreamWatermarkAssigner streamWatermarkAssigner = new StreamWatermarkAssigner(rowtimeFieldWatermark);
        streamWatermarkAssigner.assignTimeStampAndWatermark(inputStream, 10L);

        verify(inputStream, times(1)).assignTimestampsAndWatermarks(any(WatermarkStrategy.class));
    }

}
