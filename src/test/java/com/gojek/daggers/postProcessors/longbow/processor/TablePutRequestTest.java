package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.request.TablePutRequest;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;

import static org.mockito.MockitoAnnotations.initMocks;

public class TablePutRequestTest {

    private String longbowData1 = "RB-9876";
    private String longbowDuration = "1d";
    private String longbowKey = "rule123#driver444";
    private Timestamp longbowRowtime = new Timestamp(1558498933);

    @Before
    public void setup() {
        initMocks(this.getClass());
    }

    @Test
    public void shouldCreateSinglePutRequest() {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime"};
        Row input = new Row(4);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);

        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        TablePutRequest tablePutRequest = new TablePutRequest(longbowSchema, input);
        Put expectedPut = new Put(Bytes.toBytes(longbowKey + "#9223372035296276874")).addColumn(Bytes.toBytes("ts"),
                Bytes.toBytes("longbow_data1"), longbowRowtime.getTime(), Bytes.toBytes(longbowData1));

        Assert.assertEquals(new String(expectedPut.getRow()), new String(tablePutRequest.get().getRow()));
        tablePutRequest.get().get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1"));
    }


    @Test
    public void shouldCreateMultiplePutRequest() {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        Row input = new Row(5);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);
        String longbowData2 = "RB-4321";
        input.setField(4, longbowData2);

        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        TablePutRequest tablePutRequest = new TablePutRequest(longbowSchema, input);
        Put expectedPut = new Put(Bytes.toBytes(longbowKey + "#9223372035296276874")).addColumn(Bytes.toBytes("ts"),
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
