package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.postProcessors.longbow.validator.LongbowType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.gojek.daggers.utils.Constants.*;
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
        Configuration configuration = new Configuration();
        String inputProtoClassName = "com.gojek.booking.BookingLogMessage";
        AppendMetaData appendMetaData = new AppendMetaData(configuration, inputProtoClassName, longbowSchema);
        Row inputRow = new Row(1);
        String mockedValue = "order_123_4312";
        when(longbowSchema.getValue(inputRow, LongbowType.LongbowWrite.getKeyName())).thenReturn(mockedValue);

        Row appendedRow = appendMetaData.map(inputRow);

        assertEquals(LONGBOW_GCP_INSTANCE_ID_DEFAULT, appendedRow.getField(1));
        assertEquals(LONGBOW_GCP_PROJECT_ID_DEFAULT, appendedRow.getField(2));
        assertEquals(DAGGER_NAME_DEFAULT, appendedRow.getField(3));
        assertEquals(inputProtoClassName, appendedRow.getField(4));
        assertEquals(mockedValue, appendedRow.getField(5));
    }
}
