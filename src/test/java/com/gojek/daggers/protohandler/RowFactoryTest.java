package com.gojek.daggers.protohandler;

import com.gojek.esb.customer.CustomerLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.gojek.esb.types.GenderTypeProto.GenderType.Enum.MALE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RowFactoryTest {

    @Test
    public void shouldCreateRowForInputMap() {
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
    public void shouldReturnARowOfSizeEqualToNoOfFieldsInDescriptorForInputMap() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        Row row = RowFactory.createRow(inputMap, descriptor);
        assertEquals(29, row.getArity());
    }

    @Test
    public void shouldReturnEmptyRowIfNoFieldsPassedForInputMap() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        Row row = RowFactory.createRow(inputMap, descriptor);
        for (int index = 0; index < row.getArity(); index++) {
            assertEquals(null, row.getField(index));
        }
    }

    @Test
    public void shouldCreateRowWithPassedFieldsForInputMap() {
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
    public void shouldReturnEmptyRowIfNullPassedAsMapForInputMap() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Row row = RowFactory.createRow(null, descriptor);
        for (int index = 0; index < row.getArity(); index++) {
            assertEquals(null, row.getField(index));
        }
    }

    @Test
    public void shouldCreateRowForDynamicMessage() throws InvalidProtocolBufferException {
        CustomerLogMessage customerLogMessage = CustomerLogMessage.newBuilder().build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(CustomerLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage);
        assertNotNull(row);
    }

    @Test
    public void shouldReturnARowOfSizeEqualToNoOfFieldsInDescriptorForDynamicMessage() throws InvalidProtocolBufferException {
        CustomerLogMessage customerLogMessage = CustomerLogMessage.newBuilder().build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(CustomerLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage);
        assertEquals(29, row.getArity());
    }

    @Test
    public void shouldCreateRowWithPSetFieldsForDynamicMessage() throws InvalidProtocolBufferException {
        CustomerLogMessage customerLogMessage = CustomerLogMessage
                .newBuilder()
                .setCustomerId("144614")
                .setCustomerUrl("https://www.abcd.com/1234")
                .setActive(true)
                .setSex(MALE)
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(CustomerLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage);
        assertEquals("144614", row.getField(0));
        assertEquals("https://www.abcd.com/1234", row.getField(1));
        assertEquals(true, row.getField(9));
        assertEquals("MALE", row.getField(11));
    }

}
