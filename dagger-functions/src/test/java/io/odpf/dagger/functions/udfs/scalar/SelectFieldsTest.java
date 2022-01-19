package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;

import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestEnrichedBookingLogMessage;
import io.odpf.dagger.functions.exceptions.LongbowException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SelectFieldsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private StencilClient stencilClient;
    @Mock
    private MetricGroup metricGroup;
    @Mock
    private FunctionContext functionContext;
    @Mock
    private CallContext callContext;
    @Mock
    private DataTypeFactory dataTypeFactory;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "SelectFields")).thenReturn(metricGroup);
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldReturnSelectedValues() throws Exception {
        String orderNumber = "test_order_number";
        ByteString testBookingLogByteString = TestBookingLogMessage.newBuilder().setOrderNumber(orderNumber).build().toByteString();

        SelectFields selectFields = new SelectFields(stencilClient);

        ByteString[] byteStrings = new ByteString[1];
        byteStrings[0] = testBookingLogByteString;

        selectFields.open(functionContext);
        Object[] selectedFields = selectFields.eval(byteStrings, "io.odpf.dagger.consumer.TestBookingLogMessage", "order_number");

        assertEquals(selectedFields[0], "test_order_number");
    }

    @Test
    public void shouldReturnSelectedValuesInListOfInputBytes() throws Exception {
        String orderNumber = "test_order_number_1";
        ByteString testBookingLogByteString = TestBookingLogMessage.newBuilder().setOrderNumber(orderNumber).build().toByteString();
        String orderNumber1 = "test_order_number_2";
        ByteString testBookingLogByteString1 = TestBookingLogMessage.newBuilder().setOrderNumber(orderNumber1).build().toByteString();

        ByteString[] byteStrings = new ByteString[2];
        byteStrings[0] = testBookingLogByteString;
        byteStrings[1] = testBookingLogByteString1;

        SelectFields selectFields = new SelectFields(stencilClient);

        selectFields.open(functionContext);
        Object[] selectedFields = selectFields.eval(byteStrings, "io.odpf.dagger.consumer.TestBookingLogMessage", "order_number");

        assertEquals(selectedFields[0], "test_order_number_1");
        assertEquals(selectedFields[1], "test_order_number_2");
        assertEquals(selectedFields.length, 2);
    }

    @Test
    public void shouldThrowErrorWhenFieldNameDoesNotMatch() throws Exception {
        thrown.expect(LongbowException.class);
        thrown.expectMessage("Key : order_number_ does not exist in Message io.odpf.dagger.consumer.TestBookingLogMessage");

        String orderNumber = "test_order_number";
        ByteString testBookingLogByteString = TestBookingLogMessage.newBuilder().setOrderNumber(orderNumber).build().toByteString();
        ByteString[] byteStrings = new ByteString[1];
        byteStrings[0] = testBookingLogByteString;

        SelectFields selectFields = new SelectFields(stencilClient);

        selectFields.open(functionContext);
        selectFields.eval(byteStrings, "io.odpf.dagger.consumer.TestBookingLogMessage", "order_number_");
    }

    @Test
    public void shouldReturnSelectedValuesAfterFilter() throws Exception {
        String orderNumber = "test_order_number";
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber(orderNumber).build();
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(testBookingLogMessage).build();

        SelectFields selectFields = new SelectFields(stencilClient);
        selectFields.open(functionContext);
        Object[] outputOrderNumbers = selectFields.eval(Collections.singletonList(dynamicMessage), "order_number");

        assertEquals(outputOrderNumbers[0], "test_order_number");
    }

    @Test
    public void shouldReturnSelectedValueForNestedFieldsInCaseOfFilter() throws Exception {
        long timeStampInSeconds = 100;

        Timestamp timestamp = Timestamp.newBuilder().setSeconds(timeStampInSeconds).build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setEventTimestamp(timestamp).build();
        TestEnrichedBookingLogMessage testEnrichedBookingLogMessage = TestEnrichedBookingLogMessage.newBuilder().setBookingLog(testBookingLogMessage).build();

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(testEnrichedBookingLogMessage).build();

        SelectFields selectFields = new SelectFields(stencilClient);
        selectFields.open(functionContext);
        Object[] locationNameList = selectFields.eval(Collections.singletonList(dynamicMessage), "booking_log.event_timestamp.seconds");
        assertEquals(locationNameList[0], timeStampInSeconds);
    }

    @Test
    public void shouldReturnSelectedValueForNestedFieldsInCaseOfSelectOnly() throws Exception {
        long timeStampInSeconds = 100;
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(timeStampInSeconds).build();

        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setEventTimestamp(timestamp).build();
        ByteString testEnrichedBookingLogByteMessage = TestEnrichedBookingLogMessage.newBuilder().setBookingLog(testBookingLogMessage).build().toByteString();

        ByteString[] byteStrings = new ByteString[1];
        byteStrings[0] = testEnrichedBookingLogByteMessage;
        SelectFields selectFields = new SelectFields(stencilClient);

        selectFields.open(functionContext);
        Object[] selectedFields = selectFields.eval(byteStrings,
                "io.odpf.dagger.consumer.TestEnrichedBookingLogMessage",
                "booking_log.event_timestamp.seconds");

        assertEquals(selectedFields[0], 100L);
    }

    @Test
    public void shouldThrowIfNoParentClassFound() throws Exception {
        thrown.expect(ClassNotFoundException.class);
        String orderNumber = "test_order_number";
        ByteString testBookingLogByteString = TestBookingLogMessage.newBuilder().setOrderNumber(orderNumber).build().toByteString();

        ByteString[] byteStrings = new ByteString[1];
        byteStrings[0] = testBookingLogByteString;
        SelectFields selectFields = new SelectFields(stencilClient);

        selectFields.open(functionContext);
        selectFields.eval(byteStrings, "io.odpf.dagger.consumer.NotTestBookingLogMessage", "order_number");
    }

    @Test
    public void shouldResolveOutPutTypeStrategyForUnresolvedTypes() {
        when(callContext.getDataTypeFactory()).thenReturn(dataTypeFactory);
        TypeStrategy outputTypeStrategy = new SelectFields(stencilClient).getTypeInference(dataTypeFactory).getOutputTypeStrategy();
        outputTypeStrategy.inferType(callContext);
        verify(dataTypeFactory, times(1)).createDataType(any(UnresolvedDataType.class));
    }
}
