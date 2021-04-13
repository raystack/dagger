package io.odpf.dagger.processors.longbow;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.processors.PostProcessorConfig;
import io.odpf.dagger.processors.types.PostProcessor;
import io.odpf.dagger.processors.longbow.columnmodifier.ColumnModifier;
import io.odpf.dagger.utils.Constants;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class LongbowProcessor implements PostProcessor {

    private AsyncProcessor asyncProcessor;
    private Configuration configuration;
    private ArrayList<RichAsyncFunction<Row, Row>> longbowRichFunctions;
    private ColumnModifier modifier;

    public LongbowProcessor(AsyncProcessor asyncProcessor, Configuration configuration, ArrayList<RichAsyncFunction<Row, Row>> longbowRichFunctions, ColumnModifier modifier) {
        this.asyncProcessor = asyncProcessor;
        this.configuration = configuration;
        this.longbowRichFunctions = longbowRichFunctions;
        this.modifier = modifier;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        DataStream<Row> inputStream = streamInfo.getDataStream();
        long longbowAsyncTimeout = configuration.getLong(Constants.LONGBOW_ASYNC_TIMEOUT_KEY, Constants.LONGBOW_ASYNC_TIMEOUT_DEFAULT);
        Integer longbowThreadCapacity = configuration.getInteger(Constants.LONGBOW_THREAD_CAPACITY_KEY, Constants.LONGBOW_THREAD_CAPACITY_DEFAULT);
        DataStream<Row> outputStream = inputStream;
        for (RichAsyncFunction<Row, Row> longbowRichFunction : longbowRichFunctions) {
            outputStream = asyncProcessor.orderedWait(outputStream, longbowRichFunction, longbowAsyncTimeout, TimeUnit.MILLISECONDS, longbowThreadCapacity);
        }
        return new StreamInfo(outputStream, modifier.modifyColumnNames(streamInfo.getColumnNames()));
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return false;
    }
}
