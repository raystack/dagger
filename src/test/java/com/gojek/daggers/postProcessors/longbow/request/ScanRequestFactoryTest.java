package com.gojek.daggers.postProcessors.longbow.request;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.row.LongbowRow;
import com.gojek.daggers.postProcessors.longbow.storage.ScanRequest;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ScanRequestFactoryTest {

    @Mock
    private LongbowSchema longbowSchema;

    @Mock
    private LongbowRow longbowRow;

    @Mock
    private Row input;

    private String tableId;

    @Before
    public void setup() {
        initMocks(this);
        tableId = "tableId";
    }

    @Test
    public void shouldCreateTableScanRequestIfLongBowTypeIsNotLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(false);
        ScanRequestFactory scanRequestFactory = new ScanRequestFactory(longbowSchema, tableId);
        ScanRequest scanRequest = scanRequestFactory.create(input, longbowRow);
        Assert.assertEquals(TableScanRequest.class, scanRequest.getClass());
    }

    @Test
    public void shouldCreateProtoByteScanRequestIfLongBowTypeIsLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(true);
        ScanRequestFactory scanRequestFactory = new ScanRequestFactory(longbowSchema, tableId);
        ScanRequest scanRequest = scanRequestFactory.create(input, longbowRow);
        Assert.assertEquals(ProtoByteScanRequest.class, scanRequest.getClass());
    }
}
