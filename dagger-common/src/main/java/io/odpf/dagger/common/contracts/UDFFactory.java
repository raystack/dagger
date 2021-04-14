package io.odpf.dagger.common.contracts;

import org.apache.flink.table.functions.UserDefinedFunction;

import java.util.Map;

public interface UDFFactory {
    void registerFunctions();

    Map<String, UserDefinedFunction> addfunctions();
}
