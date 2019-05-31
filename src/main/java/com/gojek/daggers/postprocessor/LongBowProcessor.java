package com.gojek.daggers.postprocessor;

import com.gojek.daggers.DaggerConfigurationException;
import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.longbow.LongBowReader;
import com.gojek.daggers.longbow.LongBowSchema;
import com.gojek.daggers.longbow.LongBowWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.Constants.*;

public class LongBowProcessor implements PostProcessor {

    private static final String[] REQUIRED_FIELDS = new String[]{LONGBOW_KEY, LONGBOW_DATA, LONGBOW_DURATION, ROWTIME};
    private HashMap<String, Integer> columnIndexMap;
    private Configuration configuration;

    public LongBowProcessor(Configuration configuration, String[] columnNames) {
        this.configuration = configuration;
        this.columnIndexMap = new HashMap<>();

        for (int i=0; i<columnNames.length; i++) {
            this.columnIndexMap.put(columnNames[i], i);
        }
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        validateQuery(configuration.getString(SQL_QUERY, SQL_QUERY_DEFAULT));
        LongBowSchema longBowSchema = new LongBowSchema(columnIndexMap);
        DataStream<Row> writeStream = AsyncDataStream.orderedWait(streamInfo.getDataStream(), (AsyncFunction) new LongBowWriter(configuration, longBowSchema), 5000, TimeUnit.MILLISECONDS, 40);
        DataStream<Row> readStream = AsyncDataStream.orderedWait(writeStream, (AsyncFunction) new LongBowReader(configuration, longBowSchema), 5000, TimeUnit.MILLISECONDS, 40);
        return new StreamInfo(readStream, streamInfo.getColumnNames());
    }

    private static void validateQuery(String sqlQuery) {
        for (int i = 0; i < REQUIRED_FIELDS.length; i++){
            if (!sqlQuery.contains(REQUIRED_FIELDS[i]))
                throw new DaggerConfigurationException("Missing required field: '" + REQUIRED_FIELDS[i] + "'");
        }
    }
}
