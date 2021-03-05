package com.gojek.daggers.postprocessors.longbow;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.postprocessors.PostProcessorConfig;
import com.gojek.daggers.postprocessors.common.AsyncProcessor;
import com.gojek.daggers.postprocessors.common.PostProcessor;
import com.gojek.daggers.postprocessors.longbow.columnmodifier.ColumnModifier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.utils.Constants.*;

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
        long longbowAsyncTimeout = configuration.getLong(LONGBOW_ASYNC_TIMEOUT_KEY, LONGBOW_ASYNC_TIMEOUT_DEFAULT);
        Integer longbowThreadCapacity = configuration.getInteger(LONGBOW_THREAD_CAPACITY_KEY, LONGBOW_THREAD_CAPACITY_DEFAULT);
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
