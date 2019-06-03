package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.longbow.processor.LongbowReader;
import com.gojek.daggers.longbow.LongbowSchema;
import com.gojek.daggers.longbow.processor.LongbowWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.Constants.LONGBOW_ASYNC_TIMEOUT_DEFAULT;
import static com.gojek.daggers.Constants.LONGBOW_CAPACITY_DEFAULT;

public class LongbowProcessor implements PostProcessor {

    private AsyncProcessor asyncProcessor;
    private LongbowSchema longBowSchema;
    private LongbowWriter longbowWriter;
    private LongbowReader longbowReader;

    public LongbowProcessor(LongbowWriter longbowWriter, LongbowReader longbowReader, AsyncProcessor asyncProcessor, LongbowSchema longBowSchema) {
        this.asyncProcessor = asyncProcessor;
        this.longBowSchema = longBowSchema;
        this.longbowWriter = longbowWriter;
        this.longbowReader = longbowReader;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        longBowSchema.validateMandatoryFields();
        DataStream<Row> inputStream = streamInfo.getDataStream();
        DataStream<Row> writeStream = asyncProcessor.orderedWait(inputStream, longbowWriter, LONGBOW_ASYNC_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS, LONGBOW_CAPACITY_DEFAULT);
        DataStream<Row> readStream = asyncProcessor.orderedWait(writeStream, longbowReader, LONGBOW_ASYNC_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS, LONGBOW_CAPACITY_DEFAULT);
        return new StreamInfo(readStream, streamInfo.getColumnNames());
    }


}
