package io.odpf.dagger.core.processors.longbow.request;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class TableScanRequestTest {

    private String longbowData1 = "RB-9876";
    private String longbowData2 = "RB-9877";
    private String longbowDuration = "1d";
    private String longbowKey = "rule123#driver444";
    private Timestamp longbowRowtime = new Timestamp(1558498933);
    private String tableId = "tableId";

    private byte[] startRow;
    private byte[] endRow;

    @Before
    public void setup() {
        initMocks(this);
        startRow = Bytes.toBytes("startRow");
        endRow = Bytes.toBytes("endRow");
    }

    @Test
    public void shouldCreateSingleScanRequest() {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime"};
        Row input = new Row(4);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);


        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        TableScanRequest actualTableScanRequest = new TableScanRequest(startRow, endRow, longbowSchema, tableId);
        Scan expectedScan = new Scan();
        expectedScan.withStartRow(startRow, true);
        expectedScan.withStopRow(endRow, true);
        expectedScan.addColumn(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1"));
        assertEquals(expectedScan.getFamilyMap(), actualTableScanRequest.get().getFamilyMap());
    }

    @Test
    public void shouldCreateMultipleScanRequests() {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        Row input = new Row(5);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);
        input.setField(4, longbowData2);


        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        TableScanRequest actualTableScanRequest = new TableScanRequest(startRow, endRow, longbowSchema, tableId);
        Scan expectedScan = new Scan();
        expectedScan.withStartRow(startRow, true);
        expectedScan.withStopRow(endRow, true);
        expectedScan.addColumn(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1"));
        expectedScan.addColumn(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data2"));
        assertEquals(expectedScan.getFamilyMap(), actualTableScanRequest.get().getFamilyMap());
    }
}
