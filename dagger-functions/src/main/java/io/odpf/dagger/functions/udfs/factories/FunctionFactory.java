package io.odpf.dagger.functions.udfs.factories;

import io.odpf.dagger.common.udfs.AggregateUdf;
import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.common.udfs.TableUdf;
import io.odpf.dagger.functions.udfs.aggregate.DistinctCount;
import io.odpf.dagger.functions.udfs.scalar.EndOfMonth;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import io.odpf.dagger.common.udfs.UdfFactory;
import io.odpf.dagger.functions.udfs.table.HistogramBucket;

import java.util.HashSet;

public class FunctionFactory extends UdfFactory {

    public FunctionFactory(StreamTableEnvironment streamTableEnvironment, Configuration configuration) {
        super(streamTableEnvironment, configuration);
    }

    @Override
    public HashSet<ScalarUdf> getScalarUdfs() {
        HashSet<ScalarUdf> scalarUdfs = new HashSet<>();
        scalarUdfs.add(new EndOfMonth());
        return scalarUdfs;
    }

    @Override
    public HashSet<TableUdf> getTableUdfs() {
        HashSet<TableUdf> tableUdfs = new HashSet<>();
        tableUdfs.add(new HistogramBucket());
        return tableUdfs;
    }

    @Override
    public HashSet<AggregateUdf> getAggregateUdfs() {
        HashSet<AggregateUdf> aggregateUdfs = new HashSet<>();
        aggregateUdfs.add(new DistinctCount());
        return aggregateUdfs;
    }
}
