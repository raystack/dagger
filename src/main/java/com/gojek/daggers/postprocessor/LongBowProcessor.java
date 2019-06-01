package com.gojek.daggers.postprocessor;

import com.gojek.daggers.DaggerConfigurationException;
import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.longbow.LongBowReader;
import com.gojek.daggers.longbow.LongBowSchema;
import com.gojek.daggers.longbow.LongBowStore;
import com.gojek.daggers.longbow.LongBowWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.gojek.daggers.Constants.*;

public class LongBowProcessor implements PostProcessor {

    private static final String[] REQUIRED_FIELDS = new String[]{LONGBOW_KEY, LONGBOW_DATA, LONGBOW_DURATION, EVENT_TIMESTAMP, ROWTIME};
    private final List<String> columnNames;
    private HashMap<String, Integer> columnIndexMap;
    private Configuration configuration;

    public LongBowProcessor(Configuration configuration, String[] columnNames) {
        this.configuration = configuration;
        this.columnIndexMap = new HashMap<>();
        this.columnNames = Arrays.asList(columnNames);
        for (int i = 0; i < columnNames.length; i++) {
            this.columnIndexMap.put(columnNames[i], i);
        }
    }

    @Override
    public StreamInfo process(StreamInfo streamInfo) {
        validateQuery();
        LongBowSchema longBowSchema = new LongBowSchema(columnIndexMap);
        DataStream<Row> writeStream = AsyncDataStream.orderedWait(streamInfo.getDataStream(), new LongBowWriter(configuration, longBowSchema), 5000, TimeUnit.MILLISECONDS, 40);
        DataStream<Row> readStream = AsyncDataStream.orderedWait(writeStream, new LongBowReader(configuration, longBowSchema), 5000, TimeUnit.MILLISECONDS, 40);
        return new StreamInfo(readStream, streamInfo.getColumnNames());
    }

    boolean validateQuery() {
        String missingFields = Arrays
                .stream(REQUIRED_FIELDS)
                .filter(field -> columnNames
                        .stream()
                        .noneMatch(columnName -> columnName.contains(field)))
                .collect(Collectors.joining(","));
        if(StringUtils.isNotEmpty(missingFields))
                throw new DaggerConfigurationException("Missing required field: '" + missingFields+ "'");
        return true;
    }

}
