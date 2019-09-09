package com.gojek.daggers.utils;

import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.customer.CustomerLogMessage;
import com.gojek.esb.fraud.DriverProfileFlattenLogMessage;
import com.google.protobuf.Descriptors;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.gojek.esb.types.GenderTypeProto.GenderType.Enum.MALE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        inputMap.put("created_at", "2016-01-18T08:55:26.16Z");
        Row row = RowMaker.makeRow(inputMap, descriptor);
        assertNotNull(row);
    }

    @Test
    public void rowMakerShouldFetchValueFromInputFieldForFieldDescriptorOfTypeInt32() {
        int actualValue = 1;
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");

        Object value = RowMaker.fetchTypeAppropriateValue(actualValue, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchParseableStringValueAsIntFromInputFieldForFieldDescriptorOfTypeInt32() {
        int actualValue = 1;
        String stringValue = String.valueOf(actualValue);
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");

        Object value = RowMaker.fetchTypeAppropriateValue(stringValue, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchDefaultValueIfValueNotFoundForFieldDescriptorOfTypeInt32() {
        int defaultValue = 0;
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");

        Object value = RowMaker.fetchTypeAppropriateValue(null, fieldDescriptor);
        Assert.assertEquals(defaultValue, value);
    }

    @Test
    public void rowMakerShouldFetchParsedFromInputValueForFieldDescriptorOfTypeInt64() {
        long actualValue = 1L;
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("toll_amount");

        Object value = RowMaker.fetchTypeAppropriateValue(actualValue, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("is_reblast");

        Object value = RowMaker.fetchTypeAppropriateValue(actualValue, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchParsedValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("is_reblast");

        Object value = RowMaker.fetchTypeAppropriateValue(String.valueOf(actualValue), fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeBool() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("is_reblast");

        Object value = RowMaker.fetchTypeAppropriateValue(null, fieldDescriptor);
        Assert.assertEquals(false, value);
    }

    @Test
    public void rowMakerShouldFetchParsedValueFromFieldForFieldDescriptorOfTypeFloat() {
        float actualValue = 5.1f;

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("total_unsubsidised_price");

        Object value = RowMaker.fetchTypeAppropriateValue(actualValue, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchDefaultValueWhenValueNotPresentForFieldDescriptorOfTypeFloat() {
        float actualValue = 0.0f;

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("total_unsubsidised_price");

        Object value = RowMaker.fetchTypeAppropriateValue(null, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerShouldFetchTimeStampAsStringFromFieldForFieldDescriptorOfTypeTimeStamp() {
        String actualValue = "2018-08-30T02:21:39.975107Z";

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        Object value = RowMaker.fetchTypeAppropriateValue(actualValue, fieldDescriptor);
        Assert.assertEquals(actualValue, value);
    }

    @Test
    public void rowMakerReturnsNullWhenTimeStampNotAvailableFieldDescriptorOfTypeTimeStamp() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        Object value = RowMaker.fetchTypeAppropriateValue(null, fieldDescriptor);
        Assert.assertNull(value);
    }

    @Test
    public void rowMakerShouldReturnEnumStringGivenEnumStringForFieldDescriptorOfTypeEnum() {
        String inputField = "DRIVER_FOUND";

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(inputField, fieldDescriptor);

        Assert.assertEquals("DRIVER_FOUND", value);
    }

    @Test
    public void rowMakerShouldReturnDefaultEnumStringIfNotFoundForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(null, fieldDescriptor);

        Assert.assertEquals("UNKNOWN", value);

    }

    @Test
    public void rowMakerShouldReturnDefaultEnumStringIfInputIsAEnumPositionAndNotInTheProtoDefinitionForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(-1, fieldDescriptor);

        Assert.assertEquals("UNKNOWN", value);
    }

    @Test
    public void rowMakerShouldReturnDefaultEnumStringIfInputIsAStringAndNotInTheProtoDefinitionForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue("dummy", fieldDescriptor);

        Assert.assertEquals("UNKNOWN", value);
    }

    @Test
    public void rowMakerShouldReturnDefaultEnumStringIfInputIsNullForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(null, fieldDescriptor);

        Assert.assertEquals("UNKNOWN", value);
    }

    @Test
    public void rowMakerShouldReturnEnumStringGivenEnumPositionForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = RowMaker.fetchTypeAppropriateValue(2, fieldDescriptor);

        Assert.assertEquals("DRIVER_FOUND", value);

    }

    @Test
    public void rowMakerHandleTimestampMessagesByReturningTheValueAvailableInInputMap() {
        String inputTimestamp = "2019-03-28T05:50:13Z";
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");

        Object value = RowMaker.fetchTypeAppropriateValue(inputTimestamp, fieldDescriptor);

        assertEquals(inputTimestamp, value.toString());

    }

    @Test
    public void rowMakerHandleTimestampMessagesByReturningNullForValuesNotAvailableInMap() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");

        Object value = RowMaker.fetchTypeAppropriateValue(null, fieldDescriptor);

        Assert.assertNull(value);
    }

    @Test
    public void rowMakerHandleTimestampMessagesByReturningNullForNonParseableTimeStamps() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");

        Object value = RowMaker.fetchTypeAppropriateValue("2", fieldDescriptor);

        Assert.assertNull(value);

    }

    @Test
    public void rowMakerHandleNonTimeStampMessagesByReturningNulls() {
        Descriptors.Descriptor descriptor = DriverProfileFlattenLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("metadata");

        Object value = RowMaker.fetchTypeAppropriateValue(null, fieldDescriptor);

        Assert.assertNull(value);
    }
}
