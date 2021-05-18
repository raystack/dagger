package io.odpf.dagger.core.processors.transformers;

public class TransformerUtils {
    enum DefaultArgument {
        INPUT_SCHEMA_TABLE("table_name");
        private final String argument;

        DefaultArgument(String argument) {
            this.argument = argument;
        }

        @Override
        public String toString() {
            return this.argument;
        }
    }

    protected static void populateDefaultArguments(TransformProcessor processor) {
        for (TransformConfig config : processor.transformConfigs) {
            config.validateFields();
            config.getTransformationArguments().put(DefaultArgument.INPUT_SCHEMA_TABLE.toString(), processor.tableName);
        }
    }
}
