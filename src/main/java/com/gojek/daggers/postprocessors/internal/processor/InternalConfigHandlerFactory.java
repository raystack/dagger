package com.gojek.daggers.postprocessors.internal.processor;

import com.gojek.daggers.postprocessors.common.ColumnNameManager;
import com.gojek.daggers.postprocessors.internal.InternalSourceConfig;
import com.gojek.daggers.postprocessors.internal.processor.constant.ConstantInternalConfigProcessor;
import com.gojek.daggers.postprocessors.internal.processor.function.FunctionInternalConfigProcessor;
import com.gojek.daggers.postprocessors.internal.processor.invalid.InvalidInternalConfigProcessor;
import com.gojek.daggers.postprocessors.internal.processor.sql.SqlConfigTypePathParser;
import com.gojek.daggers.postprocessors.internal.processor.sql.fields.SqlInternalConfigProcessor;

import java.util.Arrays;
import java.util.List;

public class InternalConfigHandlerFactory {
    private static List<InternalConfigProcessor> getHandlers(ColumnNameManager columnNameManager, SqlConfigTypePathParser sqlPathParser, InternalSourceConfig internalSourceConfig) {
        return Arrays.asList(new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig),
                new FunctionInternalConfigProcessor(columnNameManager, internalSourceConfig),
                new ConstantInternalConfigProcessor(columnNameManager, internalSourceConfig));
    }

    public static InternalConfigProcessor getProcessor(InternalSourceConfig internalSourceConfig, ColumnNameManager columnNameManager, SqlConfigTypePathParser sqlPathParser) {
        return getHandlers(columnNameManager, sqlPathParser, internalSourceConfig)
                .stream()
                .filter(customConfigProcessor -> customConfigProcessor.canProcess(internalSourceConfig.getType()))
                .findFirst()
                .orElse(new InvalidInternalConfigProcessor(internalSourceConfig));
    }

}
