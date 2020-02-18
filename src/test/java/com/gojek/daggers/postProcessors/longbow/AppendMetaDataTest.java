package com.gojek.daggers.postProcessors.longbow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.junit.Test;

import static com.gojek.daggers.utils.Constants.*;
import static org.junit.Assert.assertEquals;

public class AppendMetaDataTest {

    @Test
    public void shouldAppendRowWithStaticMeteadata() {
        Configuration configuration = new Configuration();
        String inputProtoClassName = "com.goje.booking.BookingLogMessage";
        AppendMetaData appendMetaData = new AppendMetaData(configuration, inputProtoClassName);

        Row appendedRow = appendMetaData.map(new Row(1));

        assertEquals(LONGBOW_GCP_INSTANCE_ID_DEFAULT, appendedRow.getField(1));
        assertEquals(LONGBOW_GCP_PROJECT_ID_DEFAULT, appendedRow.getField(2));
        assertEquals(DAGGER_NAME_DEFAULT, appendedRow.getField(3));
        assertEquals(inputProtoClassName, appendedRow.getField(4));
    }
}
