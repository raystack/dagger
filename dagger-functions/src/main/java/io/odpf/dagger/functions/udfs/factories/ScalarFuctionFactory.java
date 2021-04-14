package io.odpf.dagger.functions.udfs.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import io.odpf.dagger.common.contracts.UDFFactory;
import io.odpf.dagger.functions.udfs.EndOfMonth;

import java.util.HashMap;
import java.util.Map;

public class ScalarFuctionFactory implements UDFFactory {
    private Configuration daggerConfig;
    private TableEnvironment tableEnvironment;

    public ScalarFuctionFactory(Configuration daggerConfig, TableEnvironment tableEnvironment) {
        this.daggerConfig = daggerConfig;
        this.tableEnvironment = tableEnvironment;
    }

    public void registerFunctions() {
        addfunctions().forEach((scalarFunctionName, scalarUDF) -> {
            tableEnvironment.registerFunction(scalarFunctionName, (ScalarFunction) scalarUDF);
        });
    }

    public Map<String, UserDefinedFunction> addfunctions() {
        HashMap<String, UserDefinedFunction> scalarFunctions = new HashMap();
        scalarFunctions.put("EndOfMonth", new EndOfMonth());
        return scalarFunctions;
    }
}
