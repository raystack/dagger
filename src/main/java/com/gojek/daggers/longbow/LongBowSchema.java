package com.gojek.daggers.longbow;

import com.gojek.daggers.exception.InvalidLongbowDurationException;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.gojek.daggers.Constants.*;

public class LongBowSchema implements Serializable {
    private HashMap<String, Integer> columnIndexMap;

    public LongBowSchema(HashMap<String, Integer> columnIndexMap) {
        this.columnIndexMap = columnIndexMap;
    }

    public byte[] getKey(Row input, long offset) {
        String longbowKey = (String) input.getField(columnIndexMap.get(LONGBOW_KEY));
        Timestamp rowTime = (Timestamp) input.getField(columnIndexMap.get(ROWTIME));
        long reversedTimestamp = Long.MAX_VALUE - (rowTime.getTime() - offset);
        String key = longbowKey + LONGBOW_DELIMITER + reversedTimestamp;
        return Bytes.toBytes(key);
    }

    public Integer getColumnSize() {
        return columnIndexMap.size();
    }

    public Integer getIndex(String column) {
        return columnIndexMap.get(column);
    }

    public List<String> getColumns(Predicate<Map.Entry<String, Integer>> filterCondition) {
        return columnIndexMap
                .entrySet()
                .stream()
                .filter(filterCondition)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public long getDurationInMillis(Row input) {
        String longbow_duration = (String) input.getField(columnIndexMap.get(LONGBOW_DURATION));
        String durationUnit = longbow_duration.substring(longbow_duration.length() - 1);
        Long duration = Long.valueOf(longbow_duration.substring(0, longbow_duration.length() - 1));
        switch (durationUnit) {
            case HOUR_UNIT:
                return TimeUnit.HOURS.toMillis(duration);
            case DAY_UNIT:
                return TimeUnit.DAYS.toMillis(duration);

        }
        throw new InvalidLongbowDurationException(String.format("%s is a invalid longbow duration", longbow_duration));
    }

}
