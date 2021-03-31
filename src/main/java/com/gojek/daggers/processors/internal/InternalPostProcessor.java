package com.gojek.daggers.processors.internal;

import com.gojek.daggers.processors.types.PostProcessor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.processors.PostProcessorConfig;
import com.gojek.daggers.processors.ColumnNameManager;
import com.gojek.daggers.processors.types.Validator;
import com.gojek.daggers.processors.types.StreamDecorator;
import com.gojek.daggers.processors.internal.processor.InternalConfigHandlerFactory;
import com.gojek.daggers.processors.internal.processor.InternalConfigProcessor;
import com.gojek.daggers.processors.internal.processor.sql.SqlConfigTypePathParser;

public class InternalPostProcessor implements PostProcessor {

    private PostProcessorConfig postProcessorConfig;

    public InternalPostProcessor(PostProcessorConfig postProcessorConfig) {
        this.postProcessorConfig = postProcessorConfig;
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return postProcessorConfig.hasInternalSource();
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

    protected StreamDecorator getInternalDecorator(InternalSourceConfig internalSourceConfig, ColumnNameManager columnNameManager) {
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        InternalConfigProcessor processor = InternalConfigHandlerFactory
                .getProcessor(internalSourceConfig, columnNameManager, sqlPathParser);
        return new InternalDecorator(internalSourceConfig, processor, columnNameManager);
    }
}
