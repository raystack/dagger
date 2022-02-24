package io.odpf.dagger.core.processors.internal.processor.function.functions;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.protohandler.RowFactory;
import io.odpf.dagger.consumer.*;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.stencil.client.StencilClient;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class JsonPayloadFunctionTest {
    @Mock
    private InternalSourceConfig commonInternalSourceConfig;
    @Mock
    private SchemaConfig commonSchemaConfig;
    @Mock
    private RowManager commonRowManager;

    private TestMessage commonMessage;

    @Before
    public void setup() throws InvalidProtocolBufferException {
        initMocks(this);
        commonInternalSourceConfig = getInternalSourceConfigForProtoClass("io.odpf.dagger.consumer.TestBookingLogMessage");

        commonSchemaConfig = getSchemaConfigForProtoAndDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage", TestBookingLogMessage.getDescriptor());

        TestBookingLogMessage customerLogMessage = TestBookingLogMessage.newBuilder().build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());

        commonRowManager = getRowManagerForMessage(dynamicMessage);

        commonMessage = TestMessage.newBuilder()
                .setOrderNumber("order-number-123")
                .setOrderUrl("https://order-url")
                .setOrderDetails("pickup")
                .build();
    }

    private InternalSourceConfig getInternalSourceConfigForProtoClass(String protoClass) {
        InternalSourceConfig internalSourceConfig = mock(InternalSourceConfig.class);
        Map<String, String> internalProcessorConfig = new HashMap<>();
        internalProcessorConfig.put("schema_proto_class", protoClass);
        when(internalSourceConfig.getInternalProcessorConfig())
                .thenReturn(internalProcessorConfig);

        return internalSourceConfig;
    }

    private RowManager getRowManagerForMessage(DynamicMessage message) {
        Row inputRow = RowFactory.createRow(message);

        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);

        return new RowManager(parentRow);
    }

    private SchemaConfig getSchemaConfigForProtoAndDescriptor(String protoClass, Descriptors.Descriptor descriptor) {
        SchemaConfig schemaConfig = mock(SchemaConfig.class);

        StencilClient stencilClient = mock(StencilClient.class);
        when(stencilClient.get(protoClass))
                .thenReturn(descriptor);
        StencilClientOrchestrator stencilClientOrchestrator = mock(StencilClientOrchestrator.class);
        when(stencilClientOrchestrator.getStencilClient())
                .thenReturn(stencilClient);
        when(schemaConfig.getStencilClientOrchestrator())
                .thenReturn(stencilClientOrchestrator);

        return schemaConfig;
    }

    @Test
    public void shouldThrowExceptionWhenInternalProcessorConfigIsNull() {
        InternalSourceConfig invalidInternalSourceConfig = mock(InternalSourceConfig.class);
        when(invalidInternalSourceConfig.getInternalProcessorConfig())
                .thenReturn(null);
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(invalidInternalSourceConfig, commonSchemaConfig);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> {
                   jsonPayloadFunction.getResult(commonRowManager);
                });
        assertEquals("Invalid internal source configuration: missing internal processor config",
                invalidConfigException.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenSchemaProtoClassNotFoundInInternalProcessorConfig() {
        InternalSourceConfig invalidInternalSourceConfig = mock(InternalSourceConfig.class);
        when(invalidInternalSourceConfig.getInternalProcessorConfig())
                .thenReturn(new HashMap<>());
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(invalidInternalSourceConfig, commonSchemaConfig);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> {
                    jsonPayloadFunction.getResult(commonRowManager);
                });
        assertEquals("Invalid internal source configuration: missing \"schema_proto_class\" key in internal processor config",
                invalidConfigException.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenStencilClientIsNull() {
        StencilClientOrchestrator stencilClientOrchestratorWithoutStencilClient = mock(StencilClientOrchestrator.class);
        when(stencilClientOrchestratorWithoutStencilClient.getStencilClient())
                .thenReturn(null);
        SchemaConfig schemaConfigWithoutStencilClient = mock(SchemaConfig.class);
        when(schemaConfigWithoutStencilClient.getStencilClientOrchestrator())
                .thenReturn(stencilClientOrchestratorWithoutStencilClient);
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(commonInternalSourceConfig, schemaConfigWithoutStencilClient);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> {
                        jsonPayloadFunction.getResult(commonRowManager);
                });
        assertEquals("Invalid configuration: stencil client is null",
                invalidConfigException.getMessage());
    }

    @Test
    public void shouldGetJsonPayloadAsResult() {
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(commonInternalSourceConfig, commonSchemaConfig);

        String expectedJsonPayload = "{\"service_type\":\"UNKNOWN\",\"order_number\":\"\",\"order_url\":\"\",\"status\":\"UNKNOWN\",\"event_timestamp\":{\"seconds\":0,\"nanos\":0},\"customer_id\":\"\",\"customer_url\":\"\",\"driver_id\":\"\",\"driver_url\":\"\",\"activity_source\":\"\",\"service_area_id\":\"\",\"amount_paid_by_cash\":0.0,\"driver_pickup_location\":{\"name\":\"\",\"address\":\"\",\"latitude\":0.0,\"longitude\":0.0,\"type\":\"\",\"note\":\"\",\"place_id\":\"\",\"accuracy_meter\":0.0,\"gate_id\":\"\"},\"driver_dropoff_location\":{\"name\":\"\",\"address\":\"\",\"latitude\":0.0,\"longitude\":0.0,\"type\":\"\",\"note\":\"\",\"place_id\":\"\",\"accuracy_meter\":0.0,\"gate_id\":\"\"},\"customer_email\":\"\",\"customer_name\":\"\",\"customer_phone\":\"\",\"driver_email\":\"\",\"driver_name\":\"\",\"driver_phone\":\"\",\"cancel_reason_id\":0,\"cancel_reason_description\":\"\",\"booking_creation_time\":{\"seconds\":0,\"nanos\":0},\"total_customer_discount\":0.0,\"gopay_customer_discount\":0.0,\"voucher_customer_discount\":0.0,\"pickup_time\":{\"seconds\":0,\"nanos\":0},\"driver_paid_in_cash\":0.0,\"driver_paid_in_credit\":0.0,\"vehicle_type\":\"UNKNOWN\",\"customer_total_fare_without_surge\":0,\"customer_dynamic_surge_enabled\":false,\"driver_total_fare_without_surge\":0,\"driver_dynamic_surge_enabled\":false,\"meta_array\":[],\"profile_data\":null,\"event_properties\":null,\"key_values\":null,\"cash_amount\":0.0,\"int_array_field\":[],\"metadata\":[],\"payment_option_metadata\":{\"masked_card\":\"\",\"network\":\"\"},\"test_enums\":[],\"routes\":[],\"customer_price\":0.0,\"boolean_array_field\":[],\"double_array_field\":[],\"float_array_field\":[],\"long_array_field\":[]}";
        String actualJsonPayload = (String) jsonPayloadFunction.getResult(commonRowManager);
        assertEquals(expectedJsonPayload, actualJsonPayload);
    }

    @Test
    public void shouldGetCorrectJsonPayloadForStringFields() throws InvalidProtocolBufferException {
        String protoClass = "io.odpf.dagger.consumer.TestMessage";
        InternalSourceConfig internalSourceConfig = getInternalSourceConfigForProtoClass(protoClass);
        SchemaConfig schemaConfig = getSchemaConfigForProtoAndDescriptor(protoClass, TestMessage.getDescriptor());

        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(commonMessage.getDescriptor(), commonMessage.toByteArray());
        RowManager rowManager = getRowManagerForMessage(dynamicMessage);

        String expectedJsonPayload = "{\"order_number\":\"order-number-123\",\"order_url\":\"https://order-url\",\"order_details\":\"pickup\"}";
        String actualJsonPayload = (String) jsonPayloadFunction.getResult(rowManager);

        assertEquals(expectedJsonPayload, actualJsonPayload);
    }

    @Test
    public void shouldGetCorrectJsonPayloadForNestedFields() throws InvalidProtocolBufferException {
        String protoClass = "io.odpf.dagger.consumer.TestNestedMessage";
        InternalSourceConfig internalSourceConfig = getInternalSourceConfigForProtoClass(protoClass);
        SchemaConfig schemaConfig = getSchemaConfigForProtoAndDescriptor(protoClass, TestNestedMessage.getDescriptor());

        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);

        TestNestedMessage nestedMessage = TestNestedMessage.newBuilder()
                .setNestedId("id-123")
                .setSingleMessage(commonMessage)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(nestedMessage.getDescriptor(), nestedMessage.toByteArray());
        RowManager rowManager = getRowManagerForMessage(dynamicMessage);

        String expectedJsonPayload = "{\"nested_id\":\"id-123\",\"single_message\":{\"order_number\":\"order-number-123\",\"order_url\":\"https://order-url\",\"order_details\":\"pickup\"}}";
        String actualJsonPayload = (String) jsonPayloadFunction.getResult(rowManager);

        assertEquals(expectedJsonPayload, actualJsonPayload);
    }

    @Test
    public void shouldGetCorrectJsonPayloadForRepeatedFields() throws InvalidProtocolBufferException {
        String protoClass = "io.odpf.dagger.consumer.TestNestedRepeatedMessage";
        InternalSourceConfig internalSourceConfig = getInternalSourceConfigForProtoClass(protoClass);
        SchemaConfig schemaConfig = getSchemaConfigForProtoAndDescriptor(protoClass, TestNestedRepeatedMessage.getDescriptor());

        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);

        TestNestedRepeatedMessage nestedRepeatedMessage = TestNestedRepeatedMessage.newBuilder()
                .addRepeatedMessage(commonMessage)
                .addRepeatedMessage(commonMessage)
                .addRepeatedNumberField(1)
                .addRepeatedNumberField(2)
                .setNumberField(10)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(nestedRepeatedMessage.getDescriptor(), nestedRepeatedMessage.toByteArray());
        RowManager rowManager = getRowManagerForMessage(dynamicMessage);

        String expectedJsonPayload = "{\"single_message\":{\"order_number\":\"\",\"order_url\":\"\",\"order_details\":\"\"},\"repeated_message\":[{\"order_number\":\"order-number-123\",\"order_url\":\"https://order-url\",\"order_details\":\"pickup\"},{\"order_number\":\"order-number-123\",\"order_url\":\"https://order-url\",\"order_details\":\"pickup\"}],\"number_field\":10,\"repeated_number_field\":[1,2],\"metadata\":null,\"event_timestamp\":{\"seconds\":0,\"nanos\":0},\"repeated_long_field\":[]}";
        String actualJsonPayload = (String) jsonPayloadFunction.getResult(rowManager);

        assertEquals(expectedJsonPayload, actualJsonPayload);
    }

    @Test
    public void shouldGetCorrectJsonPayloadForMapFields() throws InvalidProtocolBufferException {
        String protoClass = "io.odpf.dagger.consumer.TestMapMessage";
        InternalSourceConfig internalSourceConfig = getInternalSourceConfigForProtoClass(protoClass);
        SchemaConfig schemaConfig = getSchemaConfigForProtoAndDescriptor(protoClass, TestMapMessage.getDescriptor());

        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);

        TestMapMessage mapMessage = TestMapMessage.newBuilder()
                .putCurrentState("foo", "bar")
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(mapMessage.getDescriptor(), mapMessage.toByteArray());
        RowManager rowManager = getRowManagerForMessage(dynamicMessage);

        String expectedJsonPayload = "{\"order_number\":\"\",\"current_state\":[{\"key\":\"foo\",\"value\":\"bar\"}]}";
        String actualJsonPayload = (String) jsonPayloadFunction.getResult(rowManager);

        assertEquals(expectedJsonPayload, actualJsonPayload);
    }

    @Test
    public void shouldGetCorrectJsonPayloadForRepeatedEnumFields() throws InvalidProtocolBufferException {
        String protoClass = "io.odpf.dagger.consumer.TestRepeatedEnumMessage";
        InternalSourceConfig internalSourceConfig = getInternalSourceConfigForProtoClass(protoClass);
        SchemaConfig schemaConfig = getSchemaConfigForProtoAndDescriptor(protoClass, TestRepeatedEnumMessage.getDescriptor());

        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);

        TestRepeatedEnumMessage repeatedEnumMessage = TestRepeatedEnumMessage.newBuilder()
                .addTestEnumsValue(0)
                .addTestEnumsValue(0)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(repeatedEnumMessage.getDescriptor(), repeatedEnumMessage.toByteArray());
        RowManager rowManager = getRowManagerForMessage(dynamicMessage);

        String expectedJsonPayload = "{\"test_enums\":[\"UNKNOWN\",\"UNKNOWN\"]}";
        String actualJsonPayload = (String) jsonPayloadFunction.getResult(rowManager);

        assertEquals(expectedJsonPayload, actualJsonPayload);
    }

    @Test
    public void shouldGetCorrectJsonPayloadForComplexFields() throws InvalidProtocolBufferException {
        String protoClass = "io.odpf.dagger.consumer.TestComplexMap";
        InternalSourceConfig internalSourceConfig = getInternalSourceConfigForProtoClass(protoClass);
        SchemaConfig schemaConfig = getSchemaConfigForProtoAndDescriptor(protoClass, TestComplexMap.getDescriptor());

        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);

        TestComplexMap complexMapMessage = TestComplexMap.newBuilder()
                .putComplexMap(1, commonMessage)
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(complexMapMessage.getDescriptor(), complexMapMessage.toByteArray());
        RowManager rowManager = getRowManagerForMessage(dynamicMessage);

        String expectedJsonPayload = "{\"complex_map\":[{\"key\":1,\"value\":{\"order_number\":\"order-number-123\",\"order_url\":\"https://order-url\",\"order_details\":\"pickup\"}}]}";
        String actualJsonPayload = (String) jsonPayloadFunction.getResult(rowManager);

        assertEquals(expectedJsonPayload, actualJsonPayload);
    }

    @Test
    public void canNotProcessWhenFunctionNameIsNull() {
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(commonInternalSourceConfig, commonSchemaConfig);
        assertFalse(jsonPayloadFunction.canProcess(null));
    }

    @Test
    public void canNotProcessWhenFunctionNameIsDifferent() {
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(commonInternalSourceConfig, commonSchemaConfig);
        assertFalse(jsonPayloadFunction.canProcess("CURRENT_TIMESTAMP"));
    }

    @Test
    public void canProcessWhenFunctionNameIsCorrect() {
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(commonInternalSourceConfig, commonSchemaConfig);
        assertTrue(jsonPayloadFunction.canProcess("JSON_PAYLOAD"));
    }
}
