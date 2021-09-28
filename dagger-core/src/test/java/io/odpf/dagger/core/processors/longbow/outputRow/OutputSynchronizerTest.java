package io.odpf.dagger.core.processors.longbow.outputRow;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

public class OutputSynchronizerTest {
    @Mock
    private Map<String, Object> scanResult;
    private LongbowSchema longbowSchema;

    @Before
    public void setup() {
        initMocks(this);
        String[] columnNames = {"longbow_write_key", "random-selected"};
        longbowSchema = new LongbowSchema(columnNames);
    }

    @Test
    public void shouldAppendRowWithStaticMetadata() {
        String inputProtoClassName = "Test";
        String tableId = "tableId";
        Row inputRow = new Row(2);
        String mockedValue = "order_123_4312";
        String mockedKey = "key-123#123";
        inputRow.setField(0, mockedKey);
        inputRow.setField(1, mockedValue);

        OutputSynchronizer outputSynchronizer = new OutputSynchronizer(longbowSchema, tableId, inputProtoClassName);
        Row synchronizer = outputSynchronizer.get(inputRow);
        Row expectedRow = Row.of(mockedKey,
                mockedValue,
                tableId,
                inputProtoClassName,
                mockedKey);
        assertEquals(expectedRow, synchronizer);

    }
}
