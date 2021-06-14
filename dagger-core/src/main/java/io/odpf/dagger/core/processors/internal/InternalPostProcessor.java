package io.odpf.dagger.core.processors.internal;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.PostProcessorConfig;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.processors.types.StreamDecorator;
import io.odpf.dagger.core.processors.types.Validator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.internal.processor.InternalConfigHandlerFactory;
import io.odpf.dagger.core.processors.internal.processor.InternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;

/**
 * The Internal post processor.
 */
public class InternalPostProcessor implements PostProcessor {

    private PostProcessorConfig postProcessorConfig;

    /**
     * Instantiates a new Internal post processor.
     *
     * @param postProcessorConfig the post processor config
     */
    public InternalPostProcessor(PostProcessorConfig postProcessorConfig) {
        this.postProcessorConfig = postProcessorConfig;
    }

    @Override
    public boolean canProcess(PostProcessorConfig config) {
        return config.hasInternalSource();
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        DataStream<Row> resultStream = streamInfo.getDataStream();
        ColumnNameManager columnNameManager = new ColumnNameManager(streamInfo.getColumnNames(), postProcessorConfig.getOutputColumnNames());

        for (InternalSourceConfig internalSourceConfig : postProcessorConfig.getInternalSource()) {
            resultStream = enrichStream(resultStream, internalSourceConfig, getInternalDecorator(internalSourceConfig, columnNameManager));
        }
        return new StreamInfo(resultStream, columnNameManager.getOutputColumnNames());
    }

    private DataStream<Row> enrichStream(DataStream<Row> resultStream, Validator configs, StreamDecorator decorator) {
        configs.validateFields();
        return decorator.decorate(resultStream);
    }

    /**
     * Gets internal decorator.
     *
     * @param internalSourceConfig the internal source config
     * @param columnNameManager    the column name manager
     * @return the internal decorator
     */
    protected StreamDecorator getInternalDecorator(InternalSourceConfig internalSourceConfig, ColumnNameManager columnNameManager) {
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        InternalConfigProcessor processor = InternalConfigHandlerFactory
                .getProcessor(internalSourceConfig, columnNameManager, sqlPathParser);
        return new InternalDecorator(internalSourceConfig, processor, columnNameManager);
    }
}
