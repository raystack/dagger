package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringPrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleStringTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(stringPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(stringPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeString() {
        String actualValue = "test";

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        Object value = stringPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeString() {
        Integer actualValue = 23;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        Object value = stringPrimitiveTypeHandler.getValue(actualValue);

        assertEquals("23", value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        Object value = stringPrimitiveTypeHandler.getValue(null);

        assertEquals("", value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.STRING, stringPrimitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(ObjectArrayTypeInfo.getInfoFor(Types.STRING), stringPrimitiveTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        ArrayList<String> inputValues = new ArrayList<>(Arrays.asList("1", "2", "3"));
        Object actualValues = stringPrimitiveTypeHandler.getArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (String[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        Object actualValues = stringPrimitiveTypeHandler.getArray(null);
        assertEquals(0, ((String[]) actualValues).length);
    }

}
