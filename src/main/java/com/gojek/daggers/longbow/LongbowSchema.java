package com.gojek.daggers.longbow;

import com.gojek.daggers.DaggerConfigurationException;
import com.gojek.daggers.exception.InvalidLongbowDurationException;
import com.gojek.daggers.longbow.row.LongbowRow;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
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

import static com.gojek.daggers.Constants.*;

public class LongbowSchema implements Serializable {
    private String[] MANDATORY_FIELDS = new String[]{LONGBOW_KEY, LONGBOW_DATA, EVENT_TIMESTAMP, ROWTIME};
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
        Timestamp rowTime = (Timestamp) input.getField(columnIndexMap.get(ROWTIME));
        long requiredTimestamp = rowTime.getTime() - offset;
        return getAbsoluteKey(input, requiredTimestamp);
    }

    public byte[] getAbsoluteKey(Row input, long timestamp) {
        String longbowKey = (String) input.getField(columnIndexMap.get(LONGBOW_KEY));
        long reversedTimestamp = Long.MAX_VALUE - timestamp;
        String key = longbowKey + LONGBOW_DELIMITER + reversedTimestamp;
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

    public long getDurationInMillis(Row input) {
        String longbow_duration = (String) input.getField(columnIndexMap.get(LONGBOW_DURATION));
        return getDurationInMillis(longbow_duration);
    }

    public long getDurationInMillis(String durationString) {
        String durationUnit = durationString.substring(durationString.length() - 1);
        Long duration = Long.valueOf(durationString.substring(0, durationString.length() - 1));
        switch (durationUnit) {
            case HOUR_UNIT:
                return TimeUnit.HOURS.toMillis(duration);
            case DAY_UNIT:
                return TimeUnit.DAYS.toMillis(duration);
        }
        throw new InvalidLongbowDurationException(String.format("'%s' is a invalid duration string", durationString));
    }

    public void validateMandatoryFields(LongbowRow longbowRow) {
        String missingFields = Arrays
                .stream(MANDATORY_FIELDS)
                .filter(field -> columnNames
                        .stream()
                        .noneMatch(columnName -> columnName.contains(field)))
                .collect(Collectors.joining(","));

        String invalidFields = Arrays.stream(longbowRow.getInvalidFields())
                .filter(field -> columnNames
                        .stream()
                        .anyMatch(columnName -> columnName.contains(field)))
                .collect(Collectors.joining(","));

        if (StringUtils.isNotEmpty(missingFields))
            throw new DaggerConfigurationException("Missing required field: '" + missingFields + "'");
        if (StringUtils.isNotEmpty(invalidFields))
            throw new DaggerConfigurationException("Invalid fields: '" + invalidFields + "'");
    }
}
