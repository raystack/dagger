package io.odpf.dagger.core.sink.log;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogSinkWriter implements SinkWriter<Row, Void, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogSinkWriter.class.getName());
    private final String[] columnNames;

    public LogSinkWriter(String[] columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public void write(Row row, Context context) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            Object field = row.getField(i);
            if (field != null) {
                map.put(columnNames[i], field.toString());
            }
        }
        LOGGER.info(map.toString());
    }

    @Override
    public List<Void> prepareCommit(boolean flush) {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
