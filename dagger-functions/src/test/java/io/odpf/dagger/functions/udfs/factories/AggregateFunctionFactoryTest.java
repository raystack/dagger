package io.odpf.dagger.functions.udfs.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import io.odpf.dagger.functions.udfs.aggregate.DistinctCount;
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
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class AggregateFunctionFactoryTest {

    @Mock
    private Configuration configuration;

    @Mock
    private StreamTableEnvironment streamTableEnvironment;

    private AggregateFunctionFactory aggregateFunctionFactory;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAddAllAggregateFunctions() {
        aggregateFunctionFactory = new AggregateFunctionFactory(configuration, streamTableEnvironment);
        Map<String, UserDefinedFunction> functions = aggregateFunctionFactory.addfunctions();
        assertEquals(1, functions.size());
    }

    @Test
    public void shouldAddAggregagteFunctions() {
        aggregateFunctionFactory = new AggregateFunctionFactory(configuration, streamTableEnvironment);
        Map<String, UserDefinedFunction> functions = aggregateFunctionFactory.addfunctions();
        assertTrue(functions.containsKey("DistinctCount"));
        assertTrue(functions.get("DistinctCount") instanceof AggregateFunction);
    }

    @Test
    public void shouldNotAddScalarOrTableFunctionsFunctions() {
        aggregateFunctionFactory = new AggregateFunctionFactory(configuration, streamTableEnvironment);
        Map<String, UserDefinedFunction> functions = aggregateFunctionFactory.addfunctions();
        assertFalse(functions.get("DistinctCount") instanceof ScalarFunction);
        assertFalse(functions.get("DistinctCount") instanceof TableFunction);
    }

    @Test
    public void shouldRegisterAllAggregateFunctions() {
        when(configuration.getString("SQL_QUERY", "")).thenReturn("SELECT DistinctCount(coulmnName) from table");
        aggregateFunctionFactory = new AggregateFunctionFactory(configuration, streamTableEnvironment);
        aggregateFunctionFactory.registerFunctions();
        verify(streamTableEnvironment, Mockito.times(1)).registerFunction(eq("DistinctCount"), any(DistinctCount.class));
        verify(streamTableEnvironment, Mockito.times(1)).registerFunction(any(String.class), any(AggregateFunction.class));
    }
}
