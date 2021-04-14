package io.odpf.dagger.functions.udfs.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import io.odpf.dagger.common.contracts.UDFFactory;
import io.odpf.dagger.functions.udfs.table.HistogramBucket;

import java.util.HashMap;
import java.util.Map;

public class TableFunctionFactory implements UDFFactory {
    private Configuration daggerConfig;
    private StreamTableEnvironment streamTableEnvironment;

    public TableFunctionFactory(Configuration daggerConfig, StreamTableEnvironment streamTableEnvironment) {
        this.daggerConfig = daggerConfig;
        this.streamTableEnvironment = streamTableEnvironment;
    }

    @Override
    public void registerFunctions() {
        addfunctions().forEach((tableFunctionName, tableUDF) -> streamTableEnvironment
                .registerFunction(tableFunctionName, (TableFunction) tableUDF));
    }

    @Override
    public Map<String, UserDefinedFunction> addfunctions() {
        HashMap<String, UserDefinedFunction> tableFunctionMap = new HashMap();
        tableFunctionMap.put("HistogramBucket", new HistogramBucket());
        return tableFunctionMap;
    }
}
