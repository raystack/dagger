package io.odpf.dagger.sink.log;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LogSink extends RichSinkFunction<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogSink.class.getName());

    private String[] columnNames;

    public LogSink(String[] columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {

        Map map = new HashMap<String, String>();
        for (int i = 0; i < columnNames.length; i++) {
            Object field = row.getField(i);
            if (field != null) {
                map.put(columnNames[i], field.toString());
            }
        }
        LOGGER.info(map.toString());
    }
}
