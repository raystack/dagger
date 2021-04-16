package io.odpf.dagger.functions.udfs.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import io.odpf.dagger.functions.udfs.scalar.EndOfMonth;
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

public class ScalarFuctionFactoryTest {

    @Mock
    private Configuration configuration;

    @Mock
    private StreamTableEnvironment streamTableEnvironment;

    private ScalarFuctionFactory scalarFuctionFactory;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAddAllScalarFunctions() {
        scalarFuctionFactory = new ScalarFuctionFactory(configuration, streamTableEnvironment);
        Map<String, UserDefinedFunction> functions = scalarFuctionFactory.addfunctions();
        assertEquals(1, functions.size());
    }

    @Test
    public void shouldAddScalarFunctions() {
        scalarFuctionFactory = new ScalarFuctionFactory(configuration, streamTableEnvironment);
        Map<String, UserDefinedFunction> functions = scalarFuctionFactory.addfunctions();
        assertTrue(functions.containsKey("EndOfMonth"));
        assertTrue(functions.get("EndOfMonth") instanceof ScalarFunction);
    }

    @Test
    public void shouldNotAddAggregateOrTableFunctionsFunctions() {
        scalarFuctionFactory = new ScalarFuctionFactory(configuration, streamTableEnvironment);
        Map<String, UserDefinedFunction> functions = scalarFuctionFactory.addfunctions();
        assertFalse(functions.get("EndOfMonth") instanceof AggregateFunction);
        assertFalse(functions.get("EndOfMonth") instanceof TableFunction);
    }

    @Test
    public void shouldRegisterAllScalarFunctions() {
        scalarFuctionFactory = new ScalarFuctionFactory(configuration, streamTableEnvironment);
        scalarFuctionFactory.registerFunctions();
        verify(streamTableEnvironment, Mockito.times(1)).registerFunction(eq("EndOfMonth"), any(EndOfMonth.class));
        verify(streamTableEnvironment, Mockito.times(1)).registerFunction(any(String.class), any(ScalarFunction.class));
    }
}
