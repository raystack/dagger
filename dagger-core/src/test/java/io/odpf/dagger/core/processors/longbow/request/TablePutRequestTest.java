package io.odpf.dagger.core.processors.longbow.request;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;

import static org.mockito.MockitoAnnotations.initMocks;

public class TablePutRequestTest {

    private final String longbowData1 = "RB-9876";
    private final String longbowDuration = "1d";
    private final String longbowKey = "rule123#driver444";
    private final Timestamp longbowRowtime = new Timestamp(1558498933);
    private String tableId;

    @Before
    public void setup() {
        initMocks(this.getClass());
        tableId = "test_tableId";
    }

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
        final Put expectedPut = new Put(Bytes.toBytes(longbowKey + "#9223372035296276874")).addColumn(Bytes.toBytes("ts"),
                Bytes.toBytes("longbow_data1"), longbowRowtime.getTime(), Bytes.toBytes(longbowData1));

        Assert.assertEquals(new String(expectedPut.getRow()), new String(tablePutRequest.get().getRow()));
        tablePutRequest.get().get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1"));
    }


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
        final Put expectedPut = new Put(Bytes.toBytes(longbowKey + "#9223372035296276874")).addColumn(Bytes.toBytes("ts"),
                Bytes.toBytes("longbow_data1"), longbowRowtime.getTime(), Bytes.toBytes(longbowData1))
                .addColumn(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data2"), longbowRowtime.getTime(),
                        Bytes.toBytes(longbowData2));

        Assert.assertEquals(new String(expectedPut.getRow()), new String(tablePutRequest.get().getRow()));
        Assert.assertEquals(expectedPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1")),
                tablePutRequest.get().get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1")));
        Assert.assertEquals(expectedPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data2")),
                tablePutRequest.get().get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data2")));
    }
}
