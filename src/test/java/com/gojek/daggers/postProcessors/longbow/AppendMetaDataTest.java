package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.postProcessors.longbow.validator.LongbowType;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class AppendMetaDataTest {

    @Mock
    private LongbowSchema longbowSchema;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAppendRowWithStaticMeteadata() {
        String inputProtoClassName = "com.gojek.booking.BookingLogMessage";
        String tableId = "tableId";
        AppendMetaData appendMetaData = new AppendMetaData(inputProtoClassName, longbowSchema, tableId);
        Row inputRow = new Row(1);
        String mockedValue = "order_123_4312";
        when(longbowSchema.getValue(inputRow, LongbowType.LongbowWrite.getKeyName())).thenReturn(mockedValue);

        Row appendedRow = appendMetaData.map(inputRow);

        assertEquals(tableId, appendedRow.getField(1));
        assertEquals(inputProtoClassName, appendedRow.getField(2));
        assertEquals(mockedValue, appendedRow.getField(3));
    }
}
