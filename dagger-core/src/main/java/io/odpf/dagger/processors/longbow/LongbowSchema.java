package io.odpf.dagger.processors.longbow;

import io.odpf.dagger.exception.DaggerConfigurationException;
import io.odpf.dagger.exception.InvalidLongbowDurationException;
import io.odpf.dagger.processors.longbow.validator.LongbowType;
import org.apache.flink.types.Row;

import io.odpf.dagger.utils.Constants;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class LongbowSchema implements Serializable {
    private HashMap<String, Integer> columnIndexMap;
    private List<String> columnNames;

    public LongbowSchema(String[] columnNames) {
        this.columnNames = Arrays.asList(columnNames);
        this.columnIndexMap = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            columnIndexMap.put(columnNames[i], i);
        }
    }

    public byte[] getKey(Row input, long offset) {
        Timestamp rowTime = (Timestamp) input.getField(columnIndexMap.get(Constants.ROWTIME));
        long requiredTimestamp = rowTime.getTime() - offset;
        return getAbsoluteKey(input, requiredTimestamp);
    }

    public byte[] getAbsoluteKey(Row input, long timestamp) {
        String longbowKey = (String) input.getField(columnIndexMap.get(getType().getKeyName()));
        long reversedTimestamp = Long.MAX_VALUE - timestamp;
        String key = longbowKey + Constants.LONGBOW_DELIMITER + reversedTimestamp;
        return Bytes.toBytes(key);
    }

    public Integer getColumnSize() {
        return columnIndexMap.size();
    }

    public Integer getIndex(String column) {
        return columnIndexMap.get(column);
    }

    public Object getValue(Row input, String column) {
        return input.getField(columnIndexMap.get(column));
    }

    public Boolean contains(String column) {
        return columnNames.contains(column);
    }

    public List<String> getColumnNames(Predicate<Map.Entry<String, Integer>> filterCondition) {
        return columnIndexMap
                .entrySet()
                .stream()
                .filter(filterCondition)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public List<String> getColumnNames() {
        return this.columnNames;
    }

    public long getDurationInMillis(Row input) {
        String longbow_duration = (String) input.getField(columnIndexMap.get(Constants.LONGBOW_DURATION));
        return getDurationInMillis(longbow_duration);
    }

    public long getDurationInMillis(String durationString) {
        String durationUnit = durationString.substring(durationString.length() - 1);
        long duration = Long.parseLong(durationString.substring(0, durationString.length() - 1));
        switch (durationUnit) {
            case Constants.MINUTE_UNIT:
                return TimeUnit.MINUTES.toMillis(duration);
            case Constants.HOUR_UNIT:
                return TimeUnit.HOURS.toMillis(duration);
            case Constants.DAY_UNIT:
                return TimeUnit.DAYS.toMillis(duration);
        }
        throw new InvalidLongbowDurationException(String.format("'%s' is a invalid duration string", durationString));
    }

    public LongbowType getType() {
        if (columnNames.contains(LongbowType.LongbowProcess.getKeyName())) {
            return LongbowType.LongbowProcess;
        } else if (columnNames.contains(LongbowType.LongbowWrite.getKeyName())) {
            return LongbowType.LongbowWrite;
        } else if (columnNames.contains(LongbowType.LongbowRead.getKeyName())) {
            return LongbowType.LongbowRead;
        }
        throw new DaggerConfigurationException("Unable to identify LongbowProcessor. Provide either " +
                LongbowType.LongbowProcess.getKeyName() + ", " + LongbowType.LongbowRead.getKeyName() + " or " + LongbowType.LongbowWrite.getKeyName());
    }

    public boolean isLongbowPlus() {
        return getType() != LongbowType.LongbowProcess;
    }
}
