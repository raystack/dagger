package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class EsStreamDecorator implements StreamDecorator {
    private StencilClient stencilClient;
    private EsSourceConfig esSourceConfig;
    private ColumnNameManager columnNameManager;


    public EsStreamDecorator(EsSourceConfig esSourceConfig, StencilClient stencilClient, ColumnNameManager columnNameManager) {
        this.esSourceConfig = esSourceConfig;
        this.stencilClient = stencilClient;
        this.columnNameManager = columnNameManager;
    }

    @Override
    public Boolean canDecorate() {
        return esSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        AsyncFunction asyncFunction = new EsAsyncConnector(esSourceConfig, stencilClient, columnNameManager);
        return AsyncDataStream.orderedWait(inputStream, asyncFunction, esSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, esSourceConfig.getCapacity());
    }

}
