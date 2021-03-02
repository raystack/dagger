package com.gojek.daggers.postProcessors.external.common;

import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.types.GoFoodPaymentActionProto.GoFoodPaymentAction.Enum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.gojek.esb.types.GoFoodPaymentActionProto.GoFoodPaymentAction.Enum.UNKNOWN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;


public class EndpointHandlerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private EndpointHandler endpointHandler;

    @Mock
    private MeterStatsManager meterStatsManager;

    @Mock
    private SourceConfig sourceConfig;

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private ResultFuture<Row> resultFuture;

    private String[] inputProtoClasses;
    private DescriptorManager descriptorManager;

    @Before
    public void setup() {
        initMocks(this);
        StencilClient stencilClient = StencilClientFactory.getClient();
        Mockito.when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        inputProtoClasses = new String[]{"com.gojek.esb.booking.GoFoodBookingLogMessage"};
        descriptorManager = new DescriptorManager(stencilClientOrchestrator);
    }

    @Test
    public void shouldReturnEndpointQueryVariableValuesForPrimitiveDataFromDescriptor() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("customer_id");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "123456");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        Assert.assertArrayEquals(endpointOrQueryVariablesValues, new Object[]{"123456"});
    }

    @Test
    public void shouldReturnEndpointQueryVariableValuesForPrimitiveDataIfInputColumnNamesAbsent() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("id");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "123456");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        Assert.assertArrayEquals(endpointOrQueryVariablesValues, new Object[]{"123456"});
    }

    @Test
    public void shouldReturnJsonValueOfEndpointQueryValuesIncaseOfArray() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("payment_actions");

        Row row = new Row(2);
        Row inputData = new Row(2);
        List<Row> experimentsRow = new ArrayList<>();
        Row row1 = new Row(1);
        row1.setField(0, UNKNOWN);
        Row row2 = new Row(1);
        row2.setField(0, Enum.REFUND_CUSTOMER);
        experimentsRow.add(row1);
        experimentsRow.add(row2);


        inputData.setField(1, experimentsRow);
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "payment_actions"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        Assert.assertArrayEquals(endpointOrQueryVariablesValues, new Object[]{"[\"UNKNOWN\",\"REFUND_CUSTOMER\"]"});
    }

    @Test
    public void shouldReturnJsonValueOfEndpointQueryValuesIncaseOfComplexDatatype() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("driver_pickup_location");

        Row row = new Row(2);
        Row inputData = new Row(2);

        Row locationRow = new Row(9);
        locationRow.setField(0, "test_driver");
        locationRow.setField(2, 172.5d);
        locationRow.setField(3, 175.5d);

        inputData.setField(1, locationRow);
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "driver_pickup_location"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        Assert.assertArrayEquals(endpointOrQueryVariablesValues, new Object[]{"{\"name\":\"test_driver\",\"address\":null,\"latitude\":172.5,\"longitude\":175.5,\"type\":null,\"note\":null,\"place_id\":null,\"accuracy_meter\":null,\"gate_id\":null}"});
    }

    @Test
    public void shouldReturnEndpointQueryVariableValuesForPrimitiveDataFromDescriptorInCaseOfMultipleStreams() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("customer_id");
        inputProtoClasses = new String[]{"com.gojek.esb.fraud.EnrichedBookingLogMessage", "com.gojek.esb.booking.GoFoodBookingLogMessage"};

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "123456");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        Assert.assertArrayEquals(endpointOrQueryVariablesValues, new Object[]{"123456"});
    }

    @Test
    public void shouldInferEndpointVariablesFromTheCorrectStreams() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("order_number,customer_url");
        inputProtoClasses = new String[]{"com.gojek.esb.customer.CustomerLogMessage", "com.gojek.esb.booking.GoFoodBookingLogMessage"};

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "customer_url_test");
        inputData.setField(0, "test_order_number");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_url"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        Assert.assertArrayEquals(endpointOrQueryVariablesValues, new Object[]{"test_order_number", "customer_url_test"});
    }

    @Test
    public void shouldReturnEmptyObjectIfNoQueryVariables() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("");
        inputProtoClasses = new String[]{"com.gojek.esb.customer.CustomerLogMessage", "com.gojek.esb.booking.GoFoodBookingLogMessage"};

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "customer_url_test");
        inputData.setField(0, "test_order_number");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_url"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        Assert.assertArrayEquals(endpointOrQueryVariablesValues, new Object[]{});
    }

    @Test
    public void shouldThrowErrorIfVariablesAreNotProperlyConfigures() {
        expectedException.expect(InvalidConfigurationException.class);
        expectedException.expectMessage("Column 'czx' not found as configured in the endpoint/query variable");

        Mockito.when(sourceConfig.getVariables()).thenReturn("czx");
        inputProtoClasses = new String[]{"com.gojek.esb.customer.CustomerLogMessage", "com.gojek.esb.booking.GoFoodBookingLogMessage"};

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "customer_url_test");
        inputData.setField(0, "test_order_number");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_url"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        Assert.assertArrayEquals(endpointOrQueryVariablesValues, new Object[]{});
    }

    @Test
    public void shouldThrowErrorIfInputProtoNotFound() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("driver_pickup_location");
        inputProtoClasses = new String[]{"com.gojek.esb.booking.GoFoodBookingLogMessageTest"};

        Row row = new Row(2);
        Row inputData = new Row(2);

        Row locationRow = new Row(9);
        locationRow.setField(0, "test_driver");
        locationRow.setField(2, 172.5d);
        locationRow.setField(3, 175.5d);

        inputData.setField(1, locationRow);
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "driver_pickup_location"}), descriptorManager);
        try {
            endpointHandler.getEndpointOrQueryVariablesValues(rowManager, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }
        verify(errorReporter, times(1)).reportFatalException(any(DescriptorNotFoundException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(DescriptorNotFoundException.class));
    }

    @Test
    public void shouldCheckIfQueryIsValid() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("customer_id");
        Mockito.when(sourceConfig.getPattern()).thenReturn("\"{\\\"key\\\": \\\"%s\\\"}\"");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "123456");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        boolean queryInvalid = endpointHandler.isQueryInvalid(resultFuture, rowManager, endpointOrQueryVariablesValues);
        Assert.assertFalse(queryInvalid);
    }

    @Test
    public void test() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("customer_id");
        Mockito.when(sourceConfig.getPattern()).thenReturn("");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "123456");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        boolean queryInvalid = endpointHandler.isQueryInvalid(resultFuture, rowManager, endpointOrQueryVariablesValues);
        Assert.assertFalse(queryInvalid);
    }

    @Test
    public void shouldCheckIfQueryIsInValidInCaseOfSingeEmptyVariableValueForSingleField() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("customer_id");
        Mockito.when(sourceConfig.getPattern()).thenReturn("\"{\\\"key\\\": \\\"%s\\\"}\"");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        boolean queryInvalid = endpointHandler.isQueryInvalid(resultFuture, rowManager, endpointOrQueryVariablesValues);
        Assert.assertTrue(queryInvalid);
        verify(resultFuture, times(1)).complete(any());
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.EMPTY_INPUT);
    }

    @Test
    public void shouldCheckIfQueryIsValidInCaseOfSomeVariableValue() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("order_number,customer_id");
        Mockito.when(sourceConfig.getPattern()).thenReturn("\"{\\\"key\\\": \\\"%s\\\", \\\"other_key\\\": \\\"%s\\\"}\"");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "");
        inputData.setField(0, "test_order_number");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        boolean queryInvalid = endpointHandler.isQueryInvalid(resultFuture, rowManager, endpointOrQueryVariablesValues);
        Assert.assertFalse(queryInvalid);
    }

    @Test
    public void shouldCheckIfQueryIsInvalidInCaseOfAllVariableValues() {
        Mockito.when(sourceConfig.getVariables()).thenReturn("order_number,customer_id");
        Mockito.when(sourceConfig.getPattern()).thenReturn("\"{\\\"key\\\": \\\"%s\\\", \\\"other_key\\\": \\\"%s\\\"}\"");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "");
        inputData.setField(0, "");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[]{"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);

        boolean queryInvalid = endpointHandler.isQueryInvalid(resultFuture, rowManager, endpointOrQueryVariablesValues);
        Assert.assertTrue(queryInvalid);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.EMPTY_INPUT);
    }

    private ColumnNameManager getColumnNameManager(String[] columnNames) {
        List<String> outputColumnNames = Collections.singletonList("value");
        return new ColumnNameManager(columnNames, outputColumnNames);
    }
}
