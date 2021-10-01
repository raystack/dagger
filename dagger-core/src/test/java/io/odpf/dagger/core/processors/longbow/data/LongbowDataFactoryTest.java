package io.odpf.dagger.core.processors.longbow.data;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowDataFactoryTest {

    @Mock
    private LongbowSchema longbowSchema;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnLongbowTableDataWhenTypeIsNotLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(false);
        LongbowDataFactory longbowDataFactory = new LongbowDataFactory(longbowSchema);
        LongbowData longbowData = longbowDataFactory.getLongbowData();
        assertEquals(LongbowTableData.class, longbowData.getClass());
    }

    @Test
    public void shouldReturnLongbowProtoDataWhenTypeIsLongbowPlus() {
        when(longbowSchema.isLongbowPlus()).thenReturn(true);
        LongbowDataFactory longbowDataFactory = new LongbowDataFactory(longbowSchema);
        LongbowData longbowData = longbowDataFactory.getLongbowData();
        assertEquals(LongbowProtoData.class, longbowData.getClass());
    }
}
