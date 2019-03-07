package com.gojek.daggers;

import com.gojek.daggers.async.connector.ESAsyncConnector;
import com.gojek.de.stencil.StencilClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.Constants.*;

public class DeNormaliseStream {
    private DataStream dataStream;
    private Configuration configuration;
    private Table table;
    private StencilClient stencilClient;

    public DeNormaliseStream(DataStream<Row> dataStream, Configuration configuration, Table table, StencilClient stencilClient) {
        this.dataStream = dataStream;
        this.configuration = configuration;
        this.table = table;
        this.stencilClient = stencilClient;
    }

    public void apply(){
        if(!configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)) {
            dataStream.addSink(SinkFactory.getSinkFunction(configuration, table.getSchema().getColumnNames(), stencilClient));
            return;
        }
        deNormaliseUsingEs();
    }

    private void deNormaliseUsingEs() {
        NonBlockingStatsDClient statsd = new NonBlockingStatsDClient(ASYNC_IO_PREFIX,
                configuration.getString(ASYNC_IO_STATSD_HOST_KEY, ASYNC_IO_STATSD_HOST_DEFAULT),
                configuration.getInteger(ASYNC_IO_STATSD_PORT_KEY, ASYNC_IO_STATSD_PORT_DEFAULT));
        String esHost = configuration.getString(ASYNC_IO_ES_HOST_KEY, ASYNC_IO_ES_HOST_DEFAULT);
        Integer connectTimeout = configuration.getInteger(ASYNC_IO_ES_CONNECT_TIMEOUT_KEY, ASYNC_IO_ES_CONNECT_TIMEOUT_DEFAULT);
        Integer maxRetryTimeout = configuration.getInteger(ASYNC_IO_ES_MAX_RETRY_TIMEOUT_KEY, ASYNC_IO_ES_MAX_RETRY_TIMEOUT_DEFAULT);
        Integer socketTimeout = configuration.getInteger(ASYNC_IO_ES_SOCKET_TIMEOUT_KEY, ASYNC_IO_ES_SOCKET_TIMEOUT_DEFAULT);
        ESAsyncConnector esConnector = new ESAsyncConnector(esHost, connectTimeout, socketTimeout, maxRetryTimeout, statsd);
        int totalTimeout = connectTimeout + socketTimeout + maxRetryTimeout;
        org.apache.flink.streaming.api.datastream.DataStream resultStream = deNormalise(this.dataStream, esConnector, totalTimeout);
        resultStream.addSink(SinkFactory.getSinkFunction(configuration, table.getSchema().getColumnNames(), stencilClient));
    }

    protected org.apache.flink.streaming.api.datastream.DataStream deNormalise(DataStream dataStream, ESAsyncConnector esConnector, int totalTimeout) {
        org.apache.flink.streaming.api.datastream.DataStream resultStream = dataStream.javaStream();
        AsyncDataStream.unorderedWait(resultStream, esConnector, totalTimeout, TimeUnit.MILLISECONDS, configuration.getInteger(ASYNC_IO_CAPCITY_KEY, ASYNC_IO_CAPCITY_DEFAULT))
                .print();
        return resultStream;
    }

}
