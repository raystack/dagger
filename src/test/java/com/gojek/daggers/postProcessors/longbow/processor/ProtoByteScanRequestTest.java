package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.request.ProtoByteScanRequest;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoByteScanRequestTest {

    private byte[] startRow;
    private byte[] endRow;

    @Before
    public void setup() {
        initMocks(this);
        startRow = Bytes.toBytes("startRow");
        endRow = Bytes.toBytes("endRow");
    }

    @Test
    public void shouldCreateProtoScanRequest() {
        ProtoByteScanRequest protoByteScanRequest = new ProtoByteScanRequest(startRow, endRow);
        Scan expectedScan = new Scan();
        expectedScan.withStartRow(startRow, true);
        expectedScan.withStopRow(endRow, true);
        expectedScan.addColumn(Bytes.toBytes("ts"), Bytes.toBytes("proto"));
        Assert.assertTrue(expectedScan.getFamilyMap().equals(protoByteScanRequest.get().getFamilyMap()));
    }
}
