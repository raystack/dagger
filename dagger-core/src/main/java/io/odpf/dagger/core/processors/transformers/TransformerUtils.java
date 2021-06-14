package io.odpf.dagger.core.processors.transformers;

/**
 * The utils of the Transformer.
 */
public class TransformerUtils {
    /**
     * The enum Default argument.
     */
    enum DefaultArgument {
        /**
         * Table name default argument.
         */
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

    /**
     * Populate default arguments.
     *
     * @param processor the processor
     */
    protected static void populateDefaultArguments(TransformProcessor processor) {
        for (TransformConfig config : processor.transformConfigs) {
            config.validateFields();
            config.getTransformationArguments().put(DefaultArgument.INPUT_SCHEMA_TABLE.toString(), processor.tableName);
        }
    }
}
