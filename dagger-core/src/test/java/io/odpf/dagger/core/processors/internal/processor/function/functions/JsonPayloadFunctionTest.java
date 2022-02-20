package io.odpf.dagger.core.processors.internal.processor.function.functions;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.protohandler.RowFactory;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.external.SchemaConfig;
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
    private InternalSourceConfig internalSourceConfig;
    @Mock
    private SchemaConfig schemaConfig;

    @Before
    public void setup() {
        initMocks(this);
        Map<String, String> functionProcessorConfig = new HashMap<>();
        functionProcessorConfig.put("schema_proto_class", "io.odpf.dagger.consumer.TestBookingLogMessage");
        when(internalSourceConfig.getFunctionProcessorConfig())
                .thenReturn(functionProcessorConfig);

        StencilClient stencilClient = mock(StencilClient.class);
        when(stencilClient.get("io.odpf.dagger.consumer.TestBookingLogMessage"))
                .thenReturn(TestBookingLogMessage.getDescriptor());
        StencilClientOrchestrator stencilClientOrchestrator = mock(StencilClientOrchestrator.class);
        when(stencilClientOrchestrator.getStencilClient())
                .thenReturn(stencilClient);
        when(schemaConfig.getStencilClientOrchestrator())
                .thenReturn(stencilClientOrchestrator);
    }

    @Test
    public void shouldThrowExceptionWhenFunctionProcessorConfigIsNull() {
        InternalSourceConfig invalidInternalSourceConfig = mock(InternalSourceConfig.class);
        when(invalidInternalSourceConfig.getFunctionProcessorConfig())
                .thenReturn(null);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> {
                    new JsonPayloadFunction(invalidInternalSourceConfig, schemaConfig);
                });
        assertEquals("Invalid internal source configuration: missing function processor config",
                invalidConfigException.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenSchemaProtoClassNotFoundInFunctionProcessorConfig() {
        InternalSourceConfig invalidInternalSourceConfig = mock(InternalSourceConfig.class);
        when(invalidInternalSourceConfig.getFunctionProcessorConfig())
                .thenReturn(new HashMap<>());

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> {
                    new JsonPayloadFunction(invalidInternalSourceConfig, schemaConfig);
                });
        assertEquals("Invalid internal source configuration: missing \"schema_proto_class\" key in function processor config",
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

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> {
                        new JsonPayloadFunction(internalSourceConfig, schemaConfigWithoutStencilClient);
                });
        assertEquals("Invalid configuration: stencil client is null",
                invalidConfigException.getMessage());
    }

    @Test
    public void shouldGetJSONPayloadAsResult() throws InvalidProtocolBufferException {
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);

        TestBookingLogMessage customerLogMessage = TestBookingLogMessage.newBuilder().build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row inputRow = RowFactory.createRow(dynamicMessage);
        assertNotNull(inputRow);

        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        String expectedJSONPayload = "{\"service_type\":\"UNKNOWN\",\"order_number\":\"\",\"order_url\":\"\",\"status\":\"UNKNOWN\",\"event_timestamp\":{\"seconds\":0,\"nanos\":0},\"customer_id\":\"\",\"customer_url\":\"\",\"driver_id\":\"\",\"driver_url\":\"\",\"activity_source\":\"\",\"service_area_id\":\"\",\"amount_paid_by_cash\":0.0,\"driver_pickup_location\":{\"name\":\"\",\"address\":\"\",\"latitude\":0.0,\"longitude\":0.0,\"type\":\"\",\"note\":\"\",\"place_id\":\"\",\"accuracy_meter\":0.0,\"gate_id\":\"\"},\"driver_dropoff_location\":{\"name\":\"\",\"address\":\"\",\"latitude\":0.0,\"longitude\":0.0,\"type\":\"\",\"note\":\"\",\"place_id\":\"\",\"accuracy_meter\":0.0,\"gate_id\":\"\"},\"customer_email\":\"\",\"customer_name\":\"\",\"customer_phone\":\"\",\"driver_email\":\"\",\"driver_name\":\"\",\"driver_phone\":\"\",\"cancel_reason_id\":0,\"cancel_reason_description\":\"\",\"booking_creation_time\":{\"seconds\":0,\"nanos\":0},\"total_customer_discount\":0.0,\"gopay_customer_discount\":0.0,\"voucher_customer_discount\":0.0,\"pickup_time\":{\"seconds\":0,\"nanos\":0},\"driver_paid_in_cash\":0.0,\"driver_paid_in_credit\":0.0,\"vehicle_type\":\"UNKNOWN\",\"customer_total_fare_without_surge\":0,\"customer_dynamic_surge_enabled\":false,\"driver_total_fare_without_surge\":0,\"driver_dynamic_surge_enabled\":false,\"meta_array\":[],\"profile_data\":null,\"event_properties\":null,\"key_values\":null,\"cash_amount\":0.0,\"int_array_field\":[],\"metadata\":[],\"payment_option_metadata\":{\"masked_card\":\"\",\"network\":\"\"},\"test_enums\":[],\"routes\":[],\"customer_price\":0.0,\"boolean_array_field\":[],\"double_array_field\":[],\"float_array_field\":[],\"long_array_field\":[]}";
        String actualJSONPayload = (String) jsonPayloadFunction.getResult(rowManager);
        assertEquals(expectedJSONPayload, actualJSONPayload);
    }

    @Test
    public void canNotProcessWhenFunctionNameIsNull() {
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);
        assertFalse(jsonPayloadFunction.canProcess(null));
    }

    @Test
    public void canNotProcessWhenFunctionNameIsDifferent() {
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);
        assertFalse(jsonPayloadFunction.canProcess("CURRENT_TIMESTAMP"));
    }

    @Test
    public void canProcessWhenFunctionNameIsCorrect() {
        JsonPayloadFunction jsonPayloadFunction = new JsonPayloadFunction(internalSourceConfig, schemaConfig);
        assertTrue(jsonPayloadFunction.canProcess("JSON_PAYLOAD"));
    }
}
