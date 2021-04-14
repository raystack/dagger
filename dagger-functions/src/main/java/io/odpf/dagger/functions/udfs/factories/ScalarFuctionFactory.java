package io.odpf.dagger.functions.udfs.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import io.odpf.dagger.common.contracts.UDFFactory;
import io.odpf.dagger.functions.udfs.scalar.EndOfMonth;

import java.util.HashMap;
import java.util.Map;

public class ScalarFuctionFactory implements UDFFactory {
    private Configuration daggerConfig;
    private StreamTableEnvironment streamTableEnvironment;

    public ScalarFuctionFactory(Configuration daggerConfig, StreamTableEnvironment streamTableEnvironment) {
        this.daggerConfig = daggerConfig;
        this.streamTableEnvironment = streamTableEnvironment;
    }

    public void registerFunctions() {
        addfunctions().forEach((scalarFunctionName, scalarUDF) -> {
            streamTableEnvironment.registerFunction(scalarFunctionName, (ScalarFunction) scalarUDF);
        });
    }

    public Map<String, UserDefinedFunction> addfunctions() {
        HashMap<String, UserDefinedFunction> scalarFunctionMap = new HashMap();
        scalarFunctionMap.put("EndOfMonth", new EndOfMonth());
        return scalarFunctionMap;
    }
}
