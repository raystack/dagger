package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.stencil.client.StencilClient;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ElementAtTest {
    private static final double CENTRAL_MONUMENT_JAKARTA_LATITUDE = -6.170024;
    private static final double CENTRAL_MONUMENT_JAKARTA_LONGITUDE = 106.8243203;
    private LinkedHashMap<String, String> protos;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private FunctionContext functionContext;

    @Mock
    private DataTypeFactory dataTypeFactory;

    @Mock
    private CallContext callContext;

    @Before
    public void setup() {
        initMocks(this);
        protos = new LinkedHashMap<>();
        protos.put("data_stream_0", "io.odpf.dagger.consumer.TestBookingLogMessage");
        protos.put("data_stream_1", "io.odpf.dagger.consumer.TestCustomerLogMessage");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("io.odpf.dagger.consumer.TestBookingLogMessage")).thenReturn(TestBookingLogMessage.getDescriptor());
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "ElementAt")).thenReturn(metricGroup);
    }

    @Test
    public void shouldReturnElementOfArrayAtGivenIndexAndPath() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);

        elementAt.open(functionContext);
        String actual = elementAt.eval(new Row[]{routeRow}, "routes", 0, "distance_in_kms");

        assertEquals(String.valueOf(21.5), actual);
    }

    @Test
    public void shouldReturnElementOfArrayForGivenTableNameAtGivenIndexAndPath() throws Exception {
        protos = new LinkedHashMap<>();
        protos.put("data_stream_0", "io.odpf.dagger.consumer.TestCustomerLogMessage");
        protos.put("booking", "io.odpf.dagger.consumer.TestBookingLogMessage");

        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);

        elementAt.open(functionContext);
        String actual = elementAt.eval(new Row[]{routeRow}, "routes", 0, "distance_in_kms", "booking");

        assertEquals(String.valueOf(21.5), actual);
    }

    @Test
    public void shouldReturnEmptyValueAtGivenIndexAndPathWhenArrayIsNotPresentInFirstStreamAndTableNameIsNotGiven() throws Exception {
        protos = new LinkedHashMap<>();
        protos.put("data_stream_0", "io.odpf.dagger.consumer.TestCustomerLogMessage");
        protos.put("booking", "io.odpf.dagger.consumer.TestBookingLogMessage");

        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);

        elementAt.open(functionContext);
        String actual = elementAt.eval(new Row[]{routeRow}, "routes", 0, "distance_in_kms");

        assertEquals("", actual);
    }

    @Test
    public void shouldReturnElementOfArrayAtGivenIndexAndNestedPath() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Row routeRow = new Row(3);
        Row locationRow = new Row(4);
        locationRow.setField(2, CENTRAL_MONUMENT_JAKARTA_LATITUDE);
        locationRow.setField(3, CENTRAL_MONUMENT_JAKARTA_LONGITUDE);
        routeRow.setField(0, locationRow);

        elementAt.open(functionContext);
        String actualLatitude = elementAt.eval(new Row[]{routeRow}, "routes", 0, "start.latitude");
        String actualLongitude = elementAt.eval(new Row[]{routeRow}, "routes", 0, "start.longitude");

        assertEquals(String.valueOf(CENTRAL_MONUMENT_JAKARTA_LATITUDE), actualLatitude);
        assertEquals(String.valueOf(CENTRAL_MONUMENT_JAKARTA_LONGITUDE), actualLongitude);
    }

    @Test
    public void shouldReturnEmptyStringForInvalidPath() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Row routeRow = new Row(3);
        Row locationRow = new Row(4);
        locationRow.setField(2, CENTRAL_MONUMENT_JAKARTA_LATITUDE);
        locationRow.setField(3, CENTRAL_MONUMENT_JAKARTA_LONGITUDE);
        routeRow.setField(0, locationRow);

        elementAt.open(functionContext);
        String actualLatitude = elementAt.eval(new Row[]{routeRow}, "routes", 0, "start.invalid");

        assertEquals("", actualLatitude);

    }

    @Test
    public void shouldReturnEmptyStringForRowArrayLengthLessThanIndex() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Row routeRow = new Row(3);
        Row locationRow = new Row(4);
        locationRow.setField(2, CENTRAL_MONUMENT_JAKARTA_LATITUDE);
        locationRow.setField(3, CENTRAL_MONUMENT_JAKARTA_LONGITUDE);
        routeRow.setField(0, locationRow);

        elementAt.open(functionContext);
        String actualLatitude = elementAt.eval(new Row[]{routeRow}, "routes", 1, "start.latitude");

        assertEquals("", actualLatitude);
    }

    @Test
    public void shouldReturnEmptyStringWhenArrayIsNull() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Row routeRow = new Row(3);
        Row locationRow = new Row(4);
        locationRow.setField(2, CENTRAL_MONUMENT_JAKARTA_LATITUDE);
        locationRow.setField(3, CENTRAL_MONUMENT_JAKARTA_LONGITUDE);
        routeRow.setField(0, locationRow);

        elementAt.open(functionContext);
        String actualLatitude = elementAt.eval(null, "routes", 1, "start.latitude");

        assertEquals("", actualLatitude);
    }

    @Test
    public void shouldReturnEmptyStringWhenArrayIsEmpty() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Row routeRow = new Row(3);
        Row locationRow = new Row(4);
        locationRow.setField(2, CENTRAL_MONUMENT_JAKARTA_LATITUDE);
        locationRow.setField(3, CENTRAL_MONUMENT_JAKARTA_LONGITUDE);
        routeRow.setField(0, locationRow);

        elementAt.open(functionContext);
        String actualLatitude = elementAt.eval(new Row[]{}, "routes", 1, "start.latitude");

        assertEquals("", actualLatitude);
    }

    @Test
    public void shouldFindElementAtGivenIndexForObjectArray() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Object[] objects = new Object[5];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";
        objects[3] = "v";
        objects[4] = "a";

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, 2);
        assertEquals("b", element);
    }

    @Test
    public void shouldReturnNullIfValueAtGivenIndexForObjectArrayIsNull() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Object[] objects = new Object[5];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";
        objects[3] = null;
        objects[4] = "a";

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, 3);
        assertEquals(null, element);
    }

    @Test
    public void shouldReturnValueAsStringForDoubleDataTypesAtGivenIndexForObjectArray() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Object[] objects = new Object[5];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";
        objects[3] = 3.0D;
        objects[4] = "a";

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, 3);
        assertEquals("3.0", element);
    }

    @Test
    public void shouldReturnNullIfValueAtGivenIndexForObjectArrayListIsNull() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        ArrayList<Object> objects = new ArrayList<>();
        objects.add("a");
        objects.add("a");
        objects.add(null);
        objects.add("d");
        objects.add("e");

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, 2);
        assertEquals(null, element);
    }

    @Test
    public void shouldReturnValueAsStringForDoubleDataTypesAtGivenIndexForObjectArrayList() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        ArrayList<Object> objects = new ArrayList<>();
        objects.add("a");
        objects.add("a");
        objects.add(3.0D);
        objects.add("d");
        objects.add("e");

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, 2);
        assertEquals("3.0", element);
    }

    @Test
    public void shouldFindElementAtGivenIndexForObjectArrayList() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        ArrayList<Object> objects = new ArrayList<>();
        objects.add("a");
        objects.add("a");
        objects.add("c");
        objects.add("d");
        objects.add("e");

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, 2);
        assertEquals("c", element);
    }


    @Test
    public void shouldFindElementAtNegativeIndexForObjectArray() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Object[] objects = new Object[5];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";
        objects[3] = "v";
        objects[4] = "a";

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, -2);
        assertEquals("v", element);
    }

    @Test
    public void shouldFindElementAtNegativeIndexForObjectArrayList() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        ArrayList<Object> objects = new ArrayList<>();
        objects.add("a");
        objects.add("a");
        objects.add("c");
        objects.add("d");
        objects.add("e");

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, -2);
        assertEquals("d", element);
    }

    @Test
    public void shouldReturnNullIfIndexOutofBoundInArray() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Object[] objects = new Object[3];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, 5);
        Assert.assertNull(element);
    }

    @Test
    public void shouldReturnNullIfIndexOutofBoundInArrayList() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        ArrayList<Object> objects = new ArrayList<>();
        objects.add("a");
        objects.add("a");
        objects.add("c");

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, 5);
        Assert.assertNull(element);
    }

    @Test
    public void shouldReturnNullIfNegativeIndexOutOfBoundInArray() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        Object[] objects = new Object[3];
        objects[0] = "a";
        objects[1] = "a";
        objects[2] = "b";

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, -5);
        Assert.assertNull(element);
    }

    @Test
    public void shouldReturnNullIfNegativeIndexOutofBoundInArrayList() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        ArrayList<Object> objects = new ArrayList<>();
        objects.add("a");
        objects.add("a");
        objects.add("c");

        elementAt.open(functionContext);
        Object element = elementAt.eval(objects, -5);
        Assert.assertNull(element);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        elementAt.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void inputStrategyTypeArgumentCount() {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        TypeInference typeInference = elementAt.getTypeInference(dataTypeFactory);
        InputTypeStrategy inputTypeStrategy = typeInference.getInputTypeStrategy();
        ArgumentCount argumentCount = inputTypeStrategy.getArgumentCount();
        assertEquals(Optional.of(2), argumentCount.getMinCount());
        assertEquals(Optional.of(5), argumentCount.getMaxCount());

        assertTrue(argumentCount.isValidCount(4));
        assertTrue(argumentCount.isValidCount(2));
        assertTrue(argumentCount.isValidCount(5));
        assertFalse(argumentCount.isValidCount(3));
    }

    @Test
    public void inputStrategyTypeForTwoArguments() {
        List<DataType> dataTypeList = new ArrayList<DataType>(5);
        dataTypeList.add(DataTypes.ARRAY(DataTypes.STRING()));
        dataTypeList.add(DataTypes.INT());
        when(callContext.getArgumentDataTypes()).thenReturn(dataTypeList);
        InputTypeStrategy inputTypeStrategy = new ElementAt(protos, stencilClientOrchestrator)
                .getTypeInference(dataTypeFactory)
                .getInputTypeStrategy();
        Optional<List<DataType>> dataTypes = inputTypeStrategy.inferInputTypes(callContext, false);
        assertEquals(dataTypeList, dataTypes.get());
    }

    @Test
    public void inputStrategyTypeForFourArguments() {
        List<DataType> dataTypeList = new ArrayList<DataType>(5);
        dataTypeList.add(DataTypes.ARRAY(DataTypes.STRING()));
        dataTypeList.add(DataTypes.INT());
        dataTypeList.add(DataTypes.INT());
        dataTypeList.add(DataTypes.INT());
        when(callContext.getArgumentDataTypes()).thenReturn(dataTypeList);
        InputTypeStrategy inputTypeStrategy = new ElementAt(protos, stencilClientOrchestrator)
                .getTypeInference(dataTypeFactory)
                .getInputTypeStrategy();
        Optional<List<DataType>> dataTypes = inputTypeStrategy.inferInputTypes(callContext, false);
        assertEquals(Arrays.asList(dataTypeList.get(0), DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING()), dataTypes.get());
    }

    @Test
    public void inputStrategyTypeForFiveArguments() {
        List<DataType> dataTypeList = new ArrayList<DataType>(5);
        dataTypeList.add(DataTypes.ARRAY(DataTypes.STRING()));
        dataTypeList.add(DataTypes.INT());
        dataTypeList.add(DataTypes.INT());
        dataTypeList.add(DataTypes.INT());
        dataTypeList.add(DataTypes.INT());
        when(callContext.getArgumentDataTypes()).thenReturn(dataTypeList);
        InputTypeStrategy inputTypeStrategy = new ElementAt(protos, stencilClientOrchestrator)
                .getTypeInference(dataTypeFactory)
                .getInputTypeStrategy();
        Optional<List<DataType>> dataTypes = inputTypeStrategy.inferInputTypes(callContext, false);
        assertEquals(Arrays.asList(dataTypeList.get(0), DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()), dataTypes.get());
    }

    @Test
    public void outputTypeStrategy() {
        ElementAt elementAt = new ElementAt(protos, stencilClientOrchestrator);
        TypeInference typeInference = elementAt.getTypeInference(dataTypeFactory);
        TypeStrategy outputTypeStrategy = typeInference.getOutputTypeStrategy();
        Optional<DataType> dataType = outputTypeStrategy.inferType(callContext);
        assertEquals(DataTypes.STRING(), dataType.get());
    }
}
