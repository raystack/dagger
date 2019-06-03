package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.longbow.LongBowReader;
import com.gojek.daggers.longbow.LongBowSchema;
import com.gojek.daggers.longbow.LongBowWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.Constants.LONGBOW_ASYNC_TIMEOUT_DEFAULT;
import static com.gojek.daggers.Constants.LONGBOW_CAPACITY_DEFAULT;

public class LongBowProcessor implements PostProcessor {

    private AsyncProcessor asyncProcessor;
    private LongBowSchema longBowSchema;
    private LongBowWriter longbowWriter;
    private LongBowReader longbowReader;

    public LongBowProcessor(LongBowWriter longbowWriter, LongBowReader longbowReader, AsyncProcessor asyncProcessor, LongBowSchema longBowSchema) {
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
