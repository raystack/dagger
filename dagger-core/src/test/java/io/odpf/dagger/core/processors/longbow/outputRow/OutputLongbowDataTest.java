package io.odpf.dagger.core.processors.longbow.outputRow;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class OutputLongbowDataTest {
    private LongbowSchema defaultLongbowSchema;
    private Map<String, Object> scanResult;
    private Row input;
    private String[] orderNumbers;

    @Before
    public void setup() {
        String[] columnNames = {"longbow_data1", "longbow_key"};
        defaultLongbowSchema = new LongbowSchema(columnNames);
        scanResult = new HashMap<>();
        orderNumbers = new String[]{"order1"};
        scanResult.put("longbow_data1", orderNumbers);

        input = new Row(2);
        input.setField(0, "order1");
        input.setField(1, "driver#123");
    }

    @Test
    public void shouldCreateRowWithSameArity() {
        ReaderOutputLongbowData outputLongbowData = new ReaderOutputLongbowData(defaultLongbowSchema);
        Row output = outputLongbowData.get(scanResult, input);
        assertEquals(2, output.getArity());
    }

    @Test
    public void shouldReplaceLongbowDataWithScanResult() {
        ReaderOutputLongbowData outputLongbowData = new ReaderOutputLongbowData(defaultLongbowSchema);
        Row output = outputLongbowData.get(scanResult, input);
        assertEquals(orderNumbers, output.getField(0));
        assertEquals("driver#123", output.getField(1));
    }

    @Test
    public void shouldHandleMultipleLongbowData() {
        String[] columnNames = {"longbow_data1", "longbow_data2", "longbow_key"};
        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        ReaderOutputLongbowData outputLongbowData = new ReaderOutputLongbowData(longbowSchema);
        String[] customerIds = new String[]{"123"};
        scanResult.put("longbow_data2", customerIds);

        input = new Row(3);
        input.setField(0, "order1");
        input.setField(1, "123");
        input.setField(2, "driver#123");
        Row output = outputLongbowData.get(scanResult, input);
        assertEquals(orderNumbers, output.getField(0));
        assertEquals(customerIds, output.getField(1));
        assertEquals("driver#123", output.getField(2));
    }

    @Test
    public void shouldPopulateOutputWithAllTheInputFieldsWhenResultIsEmpty() {
        String[] columnNames = {"longbow_data1", "longbow_data2", "longbow_key"};
        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        ReaderOutputLongbowData outputLongbowData = new ReaderOutputLongbowData(longbowSchema);
        scanResult.put("longbow_data2", new ArrayList<>());

        input = new Row(3);
        input.setField(0, "order1");
        input.setField(1, "123");
        input.setField(2, "driver#123");
        Row output = outputLongbowData.get(scanResult, input);
        assertEquals(orderNumbers, output.getField(0));
        assertEquals(new ArrayList<>(), output.getField(1));
        assertEquals("driver#123", output.getField(2));
    }
}
