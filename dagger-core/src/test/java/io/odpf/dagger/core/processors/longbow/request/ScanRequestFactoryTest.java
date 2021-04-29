package io.odpf.dagger.core.processors.longbow.request;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.range.LongbowRange;
import io.odpf.dagger.core.processors.longbow.storage.ScanRequest;
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
    private LongbowRange longbowRange;

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
        ScanRequest scanRequest = scanRequestFactory.create(input, longbowRange);
        Assert.assertEquals(TableScanRequest.class, scanRequest.getClass());
    }

    @Test
    public void shouldCreateProtoByteScanRequestIfLongBowTypeIsLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(true);
        ScanRequestFactory scanRequestFactory = new ScanRequestFactory(longbowSchema, tableId);
        ScanRequest scanRequest = scanRequestFactory.create(input, longbowRange);
        Assert.assertEquals(ProtoByteScanRequest.class, scanRequest.getClass());
    }
}
