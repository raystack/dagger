package io.odpf.dagger.functions.udfs.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import io.odpf.dagger.functions.udfs.table.HistogramBucket;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;


public class TableFunctionFactoryTest {

    @Mock
    private Configuration configuration;

    @Mock
    private StreamTableEnvironment streamTableEnvironment;

    private TableFunctionFactory tableFunctionFactory;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAddAllTableFunctions() {
        tableFunctionFactory = new TableFunctionFactory(configuration, streamTableEnvironment);
        Map<String, UserDefinedFunction> functions = tableFunctionFactory.addfunctions();
        assertEquals(1, functions.size());
    }

    @Test
    public void shouldAddTableFunctions() {
        tableFunctionFactory = new TableFunctionFactory(configuration, streamTableEnvironment);
        Map<String, UserDefinedFunction> functions = tableFunctionFactory.addfunctions();
        assertTrue(functions.containsKey("HistogramBucket"));
        assertTrue(functions.get("HistogramBucket") instanceof TableFunction);
    }

    @Test
    public void shouldNotAddAggregateOrScalarFunctionsFunctions() {
        tableFunctionFactory = new TableFunctionFactory(configuration, streamTableEnvironment);
        Map<String, UserDefinedFunction> functions = tableFunctionFactory.addfunctions();
        assertFalse(functions.get("HistogramBucket") instanceof AggregateFunction);
        assertFalse(functions.get("HistogramBucket") instanceof ScalarFunction);
    }

    @Test
    public void shouldRegisterAllScalarFunctions() {
        tableFunctionFactory = new TableFunctionFactory(configuration, streamTableEnvironment);
        tableFunctionFactory.registerFunctions();
        verify(streamTableEnvironment, Mockito.times(1)).registerFunction(eq("HistogramBucket"), any(HistogramBucket.class));
        verify(streamTableEnvironment, Mockito.times(1)).registerFunction(any(String.class), any(TableFunction.class));
    }
}
