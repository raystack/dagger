package io.odpf.dagger.core.processors.transformers;

public class TransformerUtils {
    enum DefaultArgument {
        TABLE_NAME("table_name");
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
            config.getTransformationArguments().put(DefaultArgument.TABLE_NAME.toString(), processor.tableName);
        }
    }
}
