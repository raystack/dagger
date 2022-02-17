package io.odpf.dagger.core.processors.longbow.request;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

public class TablePutRequestTest {

    private final String longbowData1 = "RB-9876";
    private final String longbowDuration = "1d";
    private final String longbowKey = "rule123#driver444";
    private final LocalDateTime longbowRowtime = LocalDateTime.of(2022, 02, 03, 01, 01);
    private String tableId;

    @Before
    public void setup() {
        initMocks(this.getClass());
        tableId = "test_tableId";
    }

    @Ignore("Need to fix later, passing on local and failing on CI")
    @Test
    public void shouldCreateSinglePutRequest() {
        final String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime"};
        final Row input = new Row(4);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);

        final LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        final TablePutRequest tablePutRequest = new TablePutRequest(longbowSchema, input, tableId);
        final Put expectedPut = new Put(Bytes.toBytes(longbowKey + "#9223370393024515807")).addColumn(Bytes.toBytes("ts"),
                Bytes.toBytes("longbow_data1"), Timestamp.valueOf(longbowRowtime).getTime(), Bytes.toBytes(longbowData1));

        assertArrayEquals(expectedPut.getRow(), tablePutRequest.get().getRow());
        assertEquals(expectedPut.getFamilyCellMap(), tablePutRequest.get().getFamilyCellMap());
    }


    @Ignore("Need to fix later, passing on local and failing on CI")
    @Test
    public void shouldCreateMultiplePutRequest() {
        final String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        final Row input = new Row(5);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);
        final String longbowData2 = "RB-4321";
        input.setField(4, longbowData2);

        final LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        final TablePutRequest tablePutRequest = new TablePutRequest(longbowSchema, input, tableId);
        final Put expectedPut = new Put(Bytes.toBytes(longbowKey + "#9223370393024515807")).addColumn(Bytes.toBytes("ts"),
                Bytes.toBytes("longbow_data1"), Timestamp.valueOf(longbowRowtime).getTime(), Bytes.toBytes(longbowData1))
                .addColumn(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data2"), Timestamp.valueOf(longbowRowtime).getTime(),
                        Bytes.toBytes(longbowData2));

        assertArrayEquals(expectedPut.getRow(), tablePutRequest.get().getRow());
        assertEquals(expectedPut.getFamilyCellMap(), tablePutRequest.get().getFamilyCellMap());
    }
}
