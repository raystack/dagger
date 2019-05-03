package com.gojek.daggers.utils;

import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.customer.CustomerLogMessage;
import com.gojek.esb.fraud.EnrichedBookingLogMessage;
import com.google.protobuf.Descriptors;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.gojek.esb.types.GenderTypeProto.GenderType.Enum.MALE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowMakerTest {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void makeRow() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("customer_id", 144614);
        inputMap.put("customer_url", "https://www.abcd.com/1234");
        inputMap.put("active", "true");
        inputMap.put("sex", MALE);
//        inputMap.put("new_user", false);
        inputMap.put("created_at", "2016-01-18T08:55:26.16Z");
        Row row = RowMaker.makeRow(inputMap, descriptor);
        assertNotNull(row);
    }

    @Test
    public void rowMakerShouldFetchValueFromInputMapForFieldDescriptorOfTypeInt32() {
        Map<String, Object> inputMap = new HashMap<>();
        int actualValue = 1;
        inputMap.put("cancel_reason_id", actualValue);
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchParseableStringValueAsIntFromInputMapForFieldDescriptorOfTypeInt32() {
        Map<String, Object> inputMap = new HashMap<>();
        int actualValue = 1;
        inputMap.put("cancel_reason_id", String.valueOf(actualValue));
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchDefaultValueIfValueNotFoundForFieldDescriptorOfTypeInt32() {
        Map<String, Object> inputMap = new HashMap<>();
        int defaultValue = 0;
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(defaultValue, value);
    }

    @Test
    public void rowMakerShouldFetchParsedFromMapValueForFieldDescriptorOfTypeInt64() {
        Map<String, Object> inputMap = new HashMap<>();
        long actualValue = 1L;
        inputMap.put("toll_amount", String.valueOf(actualValue));

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("toll_amount");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchValueForMapForFieldDescriptorOfTypeInt64() {
        Map<String, Object> inputMap = new HashMap<>();
        long actualValue = 1L;
        inputMap.put("toll_amount", actualValue);

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("toll_amount");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchValueForMapForFieldDescriptorOfTypeBool() {
        Map<String, Object> inputMap = new HashMap<>();
        boolean actualValue = true;
        inputMap.put("is_reblast", actualValue);

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("is_reblast");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchParsedValueForMapForFieldDescriptorOfTypeBool() {
        Map<String, Object> inputMap = new HashMap<>();
        boolean actualValue = true;
        inputMap.put("is_reblast", String.valueOf(actualValue));

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("is_reblast");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchDefaultValueIfValueNotPresentInMapForFieldDescriptorOfTypeBool() {
        Map<String, Object> inputMap = new HashMap<>();

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("is_reblast");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(false, value);
    }

    @Test
    public void rowMakerShouldFetchParsedValueFromMapForFieldDescriptorOfTypeFloat() {
        Map<String, Object> inputMap = new HashMap<>();
        float actualValue = 5.1f;
        inputMap.put("total_unsubsidised_price", String.valueOf(actualValue));

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("total_unsubsidised_price");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchValueFromMapForFieldDescriptorOfTypeFloat() {
        Map<String, Object> inputMap = new HashMap<>();
        float actualValue = 5.1f;
        inputMap.put("total_unsubsidised_price", actualValue);

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("total_unsubsidised_price");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchDefaultValueWhenValueNotPresentInMapForFieldDescriptorOfTypeFloat() {
        Map<String, Object> inputMap = new HashMap<>();
        float actualValue = 0.0f;

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("total_unsubsidised_price");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchTimeStampAsStringFromMapForFieldDescriptorOfTypeTimeStamp() {
        Map<String, Object> inputMap = new HashMap<>();
        String actualValue = "2018-08-30T02:21:39.975107Z";
        inputMap.put("booking_creation_time", actualValue);

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerReturnsNullWhenTimeStampNotAvailableFieldDescriptorOfTypeTimeStamp() {
        Map<String, Object> inputMap = new HashMap<>();

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
        Assert.assertNull(value);
    }

    @Test(expected = RuntimeException.class)
    public void rowMakerThrowsRunTimeExceptionForComplexMessageTypes() {
        Map<String, Object> inputMap = new HashMap<>();

        Descriptors.Descriptor descriptor = EnrichedBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_log");

        RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);
    }

    @Test
    public void rowMakerShouldReturnEnumStringGivenEnumStringForFieldDescriptorOfTypeEnum() {
        Map<String, Object> inputMap = new HashMap<>();

        inputMap.put("status", "DRIVER_FOUND");

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);

        Assert.assertEquals("DRIVER_FOUND", value);
    }

    @Test
    public void rowMakerShouldReturnDefaultEnumStringIfNotFoundForFieldDescriptorOfTypeEnum() {
        Map<String, Object> inputMap = new HashMap<>();

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);

        Assert.assertEquals("UNKNOWN", value);

    }

    @Test
    public void rowMakerShouldReturnDefaultEnumStringIfInputIsAEnumPositionAndNotInTheProtoDefinitionForFieldDescriptorOfTypeEnum() {
        Map<String, Object> inputMap = new HashMap<>();

        inputMap.put("status", -1);
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);

        Assert.assertEquals("UNKNOWN", value);

    }

    @Test
    public void rowMakerShouldReturnDefaultEnumStringIfInputIsAStringAndNotInTheProtoDefinitionForFieldDescriptorOfTypeEnum() {
        Map<String, Object> inputMap = new HashMap<>();

        inputMap.put("status", "dummy");
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);

        Assert.assertEquals("UNKNOWN", value);

    }


    @Test
    public void rowMakerShouldReturnEnumStringGivenEnumPositionForFieldDescriptorOfTypeEnum() {
        Map<String, Object> inputMap = new HashMap<>();

        inputMap.put("status", 2);
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(inputMap, fieldDescriptor);

        Assert.assertEquals("DRIVER_FOUND", value);

    }
}
