package com.gojek.daggers.postprocessors.internal;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.postprocessors.PostProcessorConfig;
import com.gojek.daggers.postprocessors.common.ColumnNameManager;
import com.gojek.daggers.postprocessors.common.PostProcessor;
import com.gojek.daggers.postprocessors.common.Validator;
import com.gojek.daggers.postprocessors.external.common.StreamDecorator;
import com.gojek.daggers.postprocessors.internal.processor.InternalConfigHandlerFactory;
import com.gojek.daggers.postprocessors.internal.processor.InternalConfigProcessor;
import com.gojek.daggers.postprocessors.internal.processor.sql.SqlConfigTypePathParser;

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
