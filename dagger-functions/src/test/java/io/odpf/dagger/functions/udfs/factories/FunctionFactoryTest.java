package io.odpf.dagger.functions.udfs.factories;

import io.odpf.dagger.common.udfs.AggregateUdf;
import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.common.udfs.TableUdf;
import io.odpf.dagger.functions.udfs.aggregate.DistinctCount;
import io.odpf.dagger.functions.udfs.scalar.EndOfMonth;
import io.odpf.dagger.functions.udfs.table.HistogramBucket;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.HashSet;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class FunctionFactoryTest {

    @Mock
    private StreamTableEnvironment streamTableEnvironment;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnScalarUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        HashSet<ScalarUdf> scalarUdfs = functionFactory.getScalarUdfs();
        Assert.assertTrue(scalarUdfs.stream().anyMatch(scalarUdf -> scalarUdf.getClass() == EndOfMonth.class));
    }

    @Test
    public void shouldRegisterScalarUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        HashSet<ScalarUdf> scalarUdfs = functionFactory.getScalarUdfs();
        functionFactory.registerFunctions();
        ArgumentCaptor<ScalarUdf> scalarUdfArgumentCaptor = ArgumentCaptor.forClass(ScalarUdf.class);
        ArgumentCaptor<String> scalarUdfNameArgumentCaptor = ArgumentCaptor.forClass(String.class);
        scalarUdfs.forEach(scalarUdf -> {
            verify(streamTableEnvironment, times(1)).registerFunction(scalarUdfNameArgumentCaptor.capture(), scalarUdfArgumentCaptor.capture());
        });
        Assert.assertTrue(scalarUdfNameArgumentCaptor.getAllValues().contains("EndOfMonth"));
        Assert.assertTrue(scalarUdfArgumentCaptor.getAllValues().stream().anyMatch(scalarUdf -> scalarUdf.getClass() == EndOfMonth.class));
    }

    @Test
    public void shouldReturnTableUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        HashSet<TableUdf> tableUdfs = functionFactory.getTableUdfs();
        Assert.assertTrue(tableUdfs.stream().anyMatch(tableUdf -> tableUdf.getClass() == HistogramBucket.class));
    }

    @Test
    public void shouldRegisterTableUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        HashSet<TableUdf> tableUdfs = functionFactory.getTableUdfs();
        functionFactory.registerFunctions();
        ArgumentCaptor<TableUdf> tableUdfArgumentCaptor = ArgumentCaptor.forClass(TableUdf.class);
        ArgumentCaptor<String> tableUdfNameArgumentCaptor = ArgumentCaptor.forClass(String.class);
        tableUdfs.forEach(scalarUdf -> {
            verify(streamTableEnvironment, times(1)).registerFunction(tableUdfNameArgumentCaptor.capture(), tableUdfArgumentCaptor.capture());
        });
        Assert.assertTrue(tableUdfNameArgumentCaptor.getAllValues().contains("HistogramBucket"));
        Assert.assertTrue(tableUdfArgumentCaptor.getAllValues().stream().anyMatch(tableUdf -> tableUdf.getClass() == HistogramBucket.class));
    }

    @Test
    public void shouldReturnAggregateUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        HashSet<AggregateUdf> aggregateUdfs = functionFactory.getAggregateUdfs();
        Assert.assertTrue(aggregateUdfs.stream().anyMatch(aggregateUdf -> aggregateUdf.getClass() == DistinctCount.class));
    }

    @Test
    public void shouldRegisterAggregateUdfs() {
        FunctionFactory functionFactory = new FunctionFactory(streamTableEnvironment, configuration);
        HashSet<AggregateUdf> aggregateUdfs = functionFactory.getAggregateUdfs();
        functionFactory.registerFunctions();
        ArgumentCaptor<AggregateUdf> aggregateUdfArgumentCaptor = ArgumentCaptor.forClass(AggregateUdf.class);
        ArgumentCaptor<String> aggregateUdfNameArgumentCaptor = ArgumentCaptor.forClass(String.class);
        aggregateUdfs.forEach(scalarUdf -> {
            verify(streamTableEnvironment, times(1)).registerFunction(aggregateUdfNameArgumentCaptor.capture(), aggregateUdfArgumentCaptor.capture());
        });
        Assert.assertTrue(aggregateUdfNameArgumentCaptor.getAllValues().contains("DistinctCount"));
        Assert.assertTrue(aggregateUdfArgumentCaptor.getAllValues().stream().anyMatch(aggregateUdf -> aggregateUdf.getClass() == DistinctCount.class));
    }

}
