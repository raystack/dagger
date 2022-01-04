package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RowFactoryTest {

    @Test
    public void shouldCreateRowForInputMap() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("customer_id", 144614);
        inputMap.put("customer_url", "https://www.abcd.com/1234");
        inputMap.put("active", "true");
        inputMap.put("sex", "male");
        inputMap.put("created_at", "2016-01-18T08:55:26.16Z");
        Row row = RowFactory.createRow(inputMap, descriptor);

        Row expectedRow = new Row(49);
        expectedRow.setField(5, "144614");
        expectedRow.setField(6, "https://www.abcd.com/1234");
        assertEquals(expectedRow, row);

    }

    @Test
    public void shouldReturnAEmptyRowOfSizeEqualToNoOfFieldsInDescriptorForInputMap() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        Row row = RowFactory.createRow(inputMap, descriptor);
        assertEquals(new Row(49), row);
    }

    @Test
    public void shouldCreateRowWithPassedFieldsForInputMap() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("order_number", "144614");
        inputMap.put("customer_url", "https://www.abcd.com/1234");
        inputMap.put("customer_dynamic_surge_enabled", true);
        inputMap.put("event_timestamp", "2016-01-18T08:55:26.16Z");
        Row row = RowFactory.createRow(inputMap, descriptor);
        assertEquals("144614", row.getField(1));
        assertEquals("https://www.abcd.com/1234", row.getField(6));
        assertEquals(true, row.getField(31));
        assertEquals("2016-01-18T08:55:26.16Z", row.getField(4));
    }

    @Test
    public void shouldReturnEmptyRowIfNullPassedAsMapForInputMap() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Row row = RowFactory.createRow(null, descriptor);
        assertEquals(new Row(49), row);
    }

    @Test
    public void shouldCreateRowForDynamicMessage() throws InvalidProtocolBufferException {
        TestBookingLogMessage customerLogMessage = TestBookingLogMessage.newBuilder().build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage);
        assertNotNull(row);
        assertEquals(49, row.getArity());
    }

    @Test
    public void shouldCreateRowWithPSetFieldsForDynamicMessage() throws InvalidProtocolBufferException {
        TestBookingLogMessage customerLogMessage = TestBookingLogMessage
                .newBuilder()
                .setCustomerId("144614")
                .setCustomerUrl("https://www.abcd.com/1234")
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage);
        assertEquals("144614", row.getField(5));
        assertEquals("https://www.abcd.com/1234", row.getField(6));
    }


    @Test
    public void shouldBeAbleToCreateAValidCopyOfTheRowCreated() throws InvalidProtocolBufferException {
        TestBookingLogMessage customerLogMessage = TestBookingLogMessage
                .newBuilder()
                .setCustomerId("144614")
                .setCustomerUrl("https://www.abcd.com/1234")
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage);
        assertEquals("144614", row.getField(5));
        assertEquals("https://www.abcd.com/1234", row.getField(6));
        ArrayList<TypeInformation<Row>> typeInformations = new ArrayList<>();
        ExecutionConfig config = new ExecutionConfig();
        TestBookingLogMessage.getDescriptor().getFields().forEach(fieldDescriptor -> {
            typeInformations.add(ProtoHandlerFactory.getProtoHandler(fieldDescriptor).getTypeInformation());
        });
        ArrayList<TypeSerializer<Row>> typeSerializers = new ArrayList<>();
        typeInformations.forEach(rowTypeInformation -> {
            typeSerializers.add(rowTypeInformation.createSerializer(config));
        });
        RowSerializer rowSerializer = new RowSerializer(typeSerializers.toArray(new TypeSerializer[0]));

        Row copy = rowSerializer.copy(row);

        assertEquals(copy.toString(), row.toString());
    }

}
