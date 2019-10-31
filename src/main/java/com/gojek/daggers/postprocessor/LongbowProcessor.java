package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.longbow.LongbowSchema;
import com.gojek.daggers.longbow.processor.LongbowReader;
import com.gojek.daggers.longbow.processor.LongbowWriter;
import com.gojek.daggers.postprocessor.parser.PostProcessorConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.Constants.*;

public class LongbowProcessor implements PostProcessor {

    private AsyncProcessor asyncProcessor;
    private LongbowSchema longbowSchema;
    private LongbowWriter longbowWriter;
    private LongbowReader longbowReader;
    private Configuration configuration;

    public LongbowProcessor(LongbowWriter longbowWriter, LongbowReader longbowReader, AsyncProcessor asyncProcessor, LongbowSchema longbowSchema, Configuration configuration) {
        this.asyncProcessor = asyncProcessor;
        this.longbowSchema = longbowSchema;
        this.longbowWriter = longbowWriter;
        this.longbowReader = longbowReader;
        this.configuration = configuration;
    }


    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        longbowSchema.validateMandatoryFields(longbowReader.getLongbowRow());
        DataStream<Row> inputStream = streamInfo.getDataStream();
        Long longbowAsyncTimeout = configuration.getLong(LONGBOW_ASYNC_TIMEOUT_KEY, LONGBOW_ASYNC_TIMEOUT_DEFAULT);
        Integer longbowThreadCapacity = configuration.getInteger(LONGBOW_THREAD_CAPACITY_KEY, LONGBOW_THREAD_CAPACITY_DEFAULT);
        DataStream<Row> writeStream = asyncProcessor.orderedWait(inputStream, longbowWriter, longbowAsyncTimeout, TimeUnit.MILLISECONDS, longbowThreadCapacity);
        DataStream<Row> readStream = asyncProcessor.orderedWait(writeStream, longbowReader, longbowAsyncTimeout, TimeUnit.MILLISECONDS, longbowThreadCapacity);
        return new StreamInfo(readStream, streamInfo.getColumnNames());
    }

    @Override
    public boolean canProcess(Configuration configuration, PostProcessorConfig postProcessorConfig) {
        return false;
    }


}
