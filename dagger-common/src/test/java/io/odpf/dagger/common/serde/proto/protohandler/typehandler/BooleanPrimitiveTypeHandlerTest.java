package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BooleanPrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleBooleanTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(booleanPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanBoolean() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(booleanPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object value = booleanPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object value = booleanPrimitiveTypeHandler.getValue(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeBool() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object value = booleanPrimitiveTypeHandler.getValue(null);

        assertEquals(false, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.BOOLEAN, booleanPrimitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.BOOLEAN), booleanPrimitiveTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        ArrayList<Boolean> inputValues = new ArrayList<>(Arrays.asList(true, false, false));
        Object actualValues = booleanPrimitiveTypeHandler.getArray(inputValues);

        assertArrayEquals(new boolean[]{true, false, false}, (boolean[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object actualValues = booleanPrimitiveTypeHandler.getArray(null);

        assertEquals(0, ((boolean[]) actualValues).length);
    }
}
