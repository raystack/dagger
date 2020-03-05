package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.utils.Constants.*;

public class LongbowReadProcessor implements PostProcessor {
    private final LongbowReader longbowReader;
    private final AsyncProcessor asyncProcessor;
    private final Configuration configuration;

    public LongbowReadProcessor(LongbowReader longbowReader, AsyncProcessor asyncProcessor, Configuration configuration) {
        this.longbowReader = longbowReader;
        this.asyncProcessor = asyncProcessor;
        this.configuration = configuration;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        DataStream<Row> inputStream = streamInfo.getDataStream();
        long longbowAsyncTimeout = configuration.getLong(LONGBOW_ASYNC_TIMEOUT_KEY, LONGBOW_ASYNC_TIMEOUT_DEFAULT);
        Integer longbowThreadCapacity = configuration.getInteger(LONGBOW_THREAD_CAPACITY_KEY, LONGBOW_THREAD_CAPACITY_DEFAULT);
        DataStream<Row> outputStream = asyncProcessor.orderedWait(inputStream, longbowReader, longbowAsyncTimeout, TimeUnit.MILLISECONDS, longbowThreadCapacity);
        return new StreamInfo(outputStream, streamInfo.getColumnNames());
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return false;
    }
}
