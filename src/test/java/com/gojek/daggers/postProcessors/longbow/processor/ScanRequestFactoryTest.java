package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ScanRequestFactoryTest {

    @Mock
    private LongbowSchema longbowSchema;

    private byte[] startRow;

    private byte[] endRow;

    @Before
    public void setup() {
        initMocks(this);
        startRow = Bytes.toBytes("startRow");
        endRow = Bytes.toBytes("endRow");
    }

    @Test
    public void shouldCreateTableScanRequestIfLongBowDataIsPresentInLongbowSchema() {
        when(longbowSchema.hasLongbowData()).thenReturn(true);
        ScanRequestFactory scanRequestFactory = new ScanRequestFactory(longbowSchema);
        ScanRequest scanRequest = scanRequestFactory.create(startRow, endRow);
        Assert.assertEquals(TableScanRequest.class, scanRequest.getClass());
    }

    @Test
    public void shouldCreateProtoByteScanRequestIfLongBowDataIsPresentInLongbowSchema() {
        when(longbowSchema.hasLongbowData()).thenReturn(false);
        ScanRequestFactory scanRequestFactory = new ScanRequestFactory(longbowSchema);
        ScanRequest scanRequest = scanRequestFactory.create(startRow, endRow);
        Assert.assertEquals(ProtoByteScanRequest.class, scanRequest.getClass());
    }
}
