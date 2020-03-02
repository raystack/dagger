package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.PostProcessorConfig;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.utils.Constants.*;

public class LongbowWriteProcessor implements PostProcessor {
    private LongbowWriter longbowWriter;
    private AsyncProcessor asyncProcessor;
    private Configuration configuration;
    private String inputProtoClassName;
    private LongbowSchema longbowSchema;
    private String tableId;

    public LongbowWriteProcessor(LongbowWriter longbowWriter, AsyncProcessor asyncProcessor, Configuration configuration, String inputProtoClassName, LongbowSchema longbowSchema, String tableId) {
        this.longbowWriter = longbowWriter;
        this.asyncProcessor = asyncProcessor;
        this.configuration = configuration;
        this.inputProtoClassName = inputProtoClassName;
        this.longbowSchema = longbowSchema;
        this.tableId = tableId;
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        DataStream<Row> inputStream = streamInfo.getDataStream();
        long longbowAsyncTimeout = configuration.getLong(LONGBOW_ASYNC_TIMEOUT_KEY, LONGBOW_ASYNC_TIMEOUT_DEFAULT);
        Integer longbowThreadCapacity = configuration.getInteger(LONGBOW_THREAD_CAPACITY_KEY, LONGBOW_THREAD_CAPACITY_DEFAULT);
        DataStream<Row> writeStream = asyncProcessor.orderedWait(inputStream, longbowWriter, longbowAsyncTimeout, TimeUnit.MILLISECONDS, longbowThreadCapacity);

        AppendMetaData appendMetaData = new AppendMetaData(inputProtoClassName, longbowSchema, tableId);
        DataStream<Row> outputStream = writeStream.map(appendMetaData);
        // TODO: Make it as part of longbow writer and use same method as longbow write
        return new StreamInfo(outputStream, appendMetaDataColumnNames(streamInfo.getColumnNames()));
    }

    private String[] appendMetaDataColumnNames(String[] inputColumnNames) {
        ArrayList<String> outputList = new ArrayList<>(Arrays.asList(inputColumnNames));
        outputList.add(SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY);
        outputList.add(SYNCHRONIZER_INPUT_CLASSNAME_KEY);
        outputList.add(SYNCHRONIZER_LONGBOWREAD_KEY);
        return outputList.toArray(new String[0]);
    }

    @Override
    public boolean canProcess(PostProcessorConfig postProcessorConfig) {
        return false;
    }
}
