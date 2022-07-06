package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.core.utils.Constants.ExternalPostProcessorVariableType;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.consumer.TestEnumType;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.types.SourceConfig;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


public class EndpointHandlerTest {

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
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        inputProtoClasses = new String[] {"io.odpf.dagger.consumer.TestBookingLogMessage"};
        descriptorManager = new DescriptorManager(stencilClientOrchestrator);
    }

    @Test
    public void shouldReturnEndpointQueryVariableValuesForPrimitiveDataFromDescriptor() {
        when(sourceConfig.getVariables()).thenReturn("customer_id");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "123456");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        assertArrayEquals(endpointOrQueryVariablesValues, new Object[] {"123456"});
    }

    @Test
    public void shouldReturnEndpointQueryVariableValuesForPrimitiveDataIfInputColumnNamesAbsent() {
        when(sourceConfig.getVariables()).thenReturn("id");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "123456");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        assertArrayEquals(endpointOrQueryVariablesValues, new Object[] {"123456"});
    }

    @Test
    public void shouldReturnJsonValueOfEndpointQueryValuesInCaseOfArray() {
        when(sourceConfig.getVariables()).thenReturn("test_enums");

        Row row = new Row(2);
        Row inputData = new Row(2);
        List<Row> experimentsRow = new ArrayList<>();
        Row row1 = new Row(1);
        row1.setField(0, TestEnumType.Enum.UNKNOWN);
        Row row2 = new Row(1);
        row2.setField(0, TestEnumType.Enum.TYPE1);
        experimentsRow.add(row1);
        experimentsRow.add(row2);


        inputData.setField(1, experimentsRow);
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "test_enums"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        assertArrayEquals(new Object[] {"[\"+I[UNKNOWN]\",\"+I[TYPE1]\"]"}, endpointOrQueryVariablesValues);
    }

    @Test
    public void shouldReturnJsonValueOfEndpointQueryValuesIncaseOfComplexDatatype() {
        when(sourceConfig.getVariables()).thenReturn("driver_pickup_location");

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

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "driver_pickup_location"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, "driver_pickup_location", resultFuture);

        assertArrayEquals(endpointOrQueryVariablesValues, new Object[] {"{\"name\":\"test_driver\",\"address\":null,\"latitude\":172.5,\"longitude\":175.5,\"type\":null,\"note\":null,\"place_id\":null,\"accuracy_meter\":null,\"gate_id\":null}"});
    }

    @Test
    public void shouldReturnEndpointQueryVariableValuesForPrimitiveDataFromDescriptorInCaseOfMultipleStreams() {
        when(sourceConfig.getVariables()).thenReturn("customer_id");
        inputProtoClasses = new String[] {"io.odpf.dagger.consumer.TestBookingLogMessage", "io.odpf.dagger.consumer.TestBookingLogMessage"};

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "123456");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        assertArrayEquals(endpointOrQueryVariablesValues, new Object[] {"123456"});
    }

    @Test
    public void shouldInferEndpointVariablesFromTheCorrectStreams() {
        when(sourceConfig.getVariables()).thenReturn("order_number,customer_url");
        inputProtoClasses = new String[] {"io.odpf.dagger.consumer.TestBookingLogMessage", "io.odpf.dagger.consumer.TestBookingLogMessage"};

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "customer_url_test");
        inputData.setField(0, "test_order_number");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "customer_url"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        assertArrayEquals(endpointOrQueryVariablesValues, new Object[] {"test_order_number", "customer_url_test"});
    }

    @Test
    public void shouldReturnEmptyObjectIfNoQueryVariables() {
        when(sourceConfig.getVariables()).thenReturn("");
        inputProtoClasses = new String[] {"io.odpf.dagger.consumer.TestBookingLogMessage", "io.odpf.dagger.consumer.TestBookingLogMessage"};

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "customer_url_test");
        inputData.setField(0, "test_order_number");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "customer_url"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        assertArrayEquals(endpointOrQueryVariablesValues, new Object[] {});
    }

    @Test
    public void shouldThrowErrorIfRequestVariablesAreNotProperlyConfigures() {
        when(sourceConfig.getVariables()).thenReturn("czx");
        inputProtoClasses = new String[] {"io.odpf.dagger.consumer.TestBookingLogMessage", "io.odpf.dagger.consumer.TestBookingLogMessage"};

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "customer_url_test");
        inputData.setField(0, "test_order_number");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "customer_url"}), descriptorManager);
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture));
        assertEquals("Column 'czx' not found as configured in the 'REQUEST_VARIABLES' variable", exception.getMessage());
    }

    @Test
    public void shouldThrowErrorIfInputProtoNotFound() {
        when(sourceConfig.getVariables()).thenReturn("driver_pickup_location");
        inputProtoClasses = new String[] {"io.odpf.dagger.consumer.TestBookingLogMessage1"};

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

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "driver_pickup_location"}), descriptorManager);

        assertThrows(NullPointerException.class,
                () -> endpointHandler.getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture));
        verify(errorReporter, times(1)).reportFatalException(any(DescriptorNotFoundException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(DescriptorNotFoundException.class));
    }

    @Test
    public void shouldCheckIfQueryIsValid() {
        when(sourceConfig.getVariables()).thenReturn("customer_id");
        when(sourceConfig.getPattern()).thenReturn("\"{\\\"key\\\": \\\"%s\\\"}\"");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "123456");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        boolean queryInvalid = endpointHandler.isQueryInvalid(resultFuture, rowManager, sourceConfig.getVariables(), endpointOrQueryVariablesValues);
        assertFalse(queryInvalid);
    }

    @Test
    public void shouldCheckIfQueryIsInValidInCaseOfSingeEmptyVariableValueForSingleField() {
        when(sourceConfig.getVariables()).thenReturn("customer_id");
        when(sourceConfig.getPattern()).thenReturn("\"{\\\"key\\\": \\\"%s\\\"}\"");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        boolean queryInvalid = endpointHandler.isQueryInvalid(resultFuture, rowManager, sourceConfig.getVariables(), endpointOrQueryVariablesValues);
        assertTrue(queryInvalid);
        verify(resultFuture, times(1)).complete(any());
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.EMPTY_INPUT);
    }

    @Test
    public void shouldCheckIfQueryIsValidInCaseOfSomeVariableValue() {
        when(sourceConfig.getVariables()).thenReturn("order_number,customer_id");
        when(sourceConfig.getPattern()).thenReturn("\"{\\\"key\\\": \\\"%s\\\", \\\"other_key\\\": \\\"%s\\\"}\"");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "");
        inputData.setField(0, "test_order_number");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        boolean queryInvalid = endpointHandler.isQueryInvalid(resultFuture, rowManager, sourceConfig.getVariables(), endpointOrQueryVariablesValues);
        assertFalse(queryInvalid);
    }

    @Test
    public void shouldCheckIfQueryIsInvalidInCaseOfAllVariableValues() {
        when(sourceConfig.getVariables()).thenReturn("order_number,customer_id");
        when(sourceConfig.getPattern()).thenReturn("\"{\\\"key\\\": \\\"%s\\\", \\\"other_key\\\": \\\"%s\\\"}\"");

        Row row = new Row(2);
        Row inputData = new Row(2);
        inputData.setField(1, "");
        inputData.setField(0, "");
        row.setField(0, inputData);
        row.setField(1, new Row(1));
        RowManager rowManager = new RowManager(row);

        endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                inputProtoClasses, getColumnNameManager(new String[] {"order_number", "customer_id"}), descriptorManager);
        Object[] endpointOrQueryVariablesValues = endpointHandler
                .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, sourceConfig.getVariables(), resultFuture);

        boolean queryInvalid = endpointHandler.isQueryInvalid(resultFuture, rowManager, sourceConfig.getVariables(), endpointOrQueryVariablesValues);
        assertTrue(queryInvalid);
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.EMPTY_INPUT);
    }

    private ColumnNameManager getColumnNameManager(String[] columnNames) {
        List<String> outputColumnNames = Collections.singletonList("value");
        return new ColumnNameManager(columnNames, outputColumnNames);
    }
}
