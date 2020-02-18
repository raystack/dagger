package com.gojek.daggers.postProcessors.common;

import com.gojek.daggers.protoHandler.RowFactory;
import com.gojek.esb.customer.CustomerLogMessage;
import com.google.protobuf.Descriptors;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.gojek.esb.types.GenderTypeProto.GenderType.Enum.MALE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RowFactoryTest {

    @Test
    public void shouldCreateRow() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("customer_id", 144614);
        inputMap.put("customer_url", "https://www.abcd.com/1234");
        inputMap.put("active", "true");
        inputMap.put("sex", MALE);
        inputMap.put("created_at", "2016-01-18T08:55:26.16Z");
        Row row = RowFactory.createRow(inputMap, descriptor);
        assertNotNull(row);
    }

    @Test
    public void shouldReturnARowOfSizeEqualToNoOfFieldsInDescriptor() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        Row row = RowFactory.createRow(inputMap, descriptor);
        assertEquals(19, row.getArity());
    }

    @Test
    public void shouldReturnEmptyRowIfNoFieldsPassed() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        Row row = RowFactory.createRow(inputMap, descriptor);
        for (int index = 0; index < row.getArity(); index++) {
            assertEquals(null, row.getField(index));
        }
    }

    @Test
    public void shouldCreateRowWithPassedFields() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("customer_id", "144614");
        inputMap.put("customer_url", "https://www.abcd.com/1234");
        inputMap.put("active", true);
        inputMap.put("sex", MALE);
        inputMap.put("created_at", "2016-01-18T08:55:26.16Z");
        Row row = RowFactory.createRow(inputMap, descriptor);
        assertEquals("144614", row.getField(0));
        assertEquals("https://www.abcd.com/1234", row.getField(1));
        assertEquals(true, row.getField(9));
        assertEquals("MALE", row.getField(11));
        assertEquals("2016-01-18T08:55:26.16Z", row.getField(17));
    }

    @Test
    public void shouldReturnEmptyRowIfNullPassedAsMap() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Row row = RowFactory.createRow(null, descriptor);
        for (int index = 0; index < row.getArity(); index++) {
            assertEquals(null, row.getField(index));
        }
    }
}
