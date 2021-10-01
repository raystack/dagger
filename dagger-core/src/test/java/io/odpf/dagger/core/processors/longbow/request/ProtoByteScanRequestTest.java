package io.odpf.dagger.core.processors.longbow.request;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoByteScanRequestTest {

    private byte[] startRow;
    private byte[] endRow;
    private String tableId = "tableId";

    @Before
    public void setup() {
        initMocks(this);
        startRow = Bytes.toBytes("startRow");
        endRow = Bytes.toBytes("endRow");
    }

    @Test
    public void shouldCreateProtoScanRequest() {
        ProtoByteScanRequest protoByteScanRequest = new ProtoByteScanRequest(startRow, endRow, tableId);
        Scan expectedScan = new Scan();
        expectedScan.withStartRow(startRow, true);
        expectedScan.withStopRow(endRow, true);
        expectedScan.addColumn(Bytes.toBytes("ts"), Bytes.toBytes("proto"));
        assertEquals(expectedScan.getFamilyMap(), protoByteScanRequest.get().getFamilyMap());
    }
}
