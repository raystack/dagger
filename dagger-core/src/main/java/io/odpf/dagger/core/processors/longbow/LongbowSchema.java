package io.odpf.dagger.core.processors.longbow;

import org.apache.flink.types.Row;

import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.exception.InvalidLongbowDurationException;
import io.odpf.dagger.core.processors.longbow.validator.LongbowType;
import io.odpf.dagger.core.utils.Constants;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.odpf.dagger.common.core.Constants.ROWTIME;

/**
 * A class that holds the Longbow schema.
 */
public class LongbowSchema implements Serializable {
    private HashMap<String, Integer> columnIndexMap;
    private List<String> columnNames;

    /**
     * Instantiates a new Longbow schema.
     *
     * @param columnNames the column names
     */
    public LongbowSchema(String[] columnNames) {
        this.columnNames = Arrays.asList(columnNames);
        this.columnIndexMap = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            columnIndexMap.put(columnNames[i], i);
        }
    }

    /**
     * Get key.
     *
     * @param input  the input
     * @param offset the offset
     * @return the key in byte array
     */
    public byte[] getKey(Row input, long offset) {
        Timestamp rowTime = convertToTimeStamp(input.getField(columnIndexMap.get(ROWTIME)));
        long requiredTimestamp = rowTime.getTime() - offset;
        return getAbsoluteKey(input, requiredTimestamp);
    }

    /**
     * Get absolute key.
     *
     * @param input     the input
     * @param timestamp the timestamp
     * @return get key in byte array
     */
    public byte[] getAbsoluteKey(Row input, long timestamp) {
        String longbowKey = (String) input.getField(columnIndexMap.get(getType().getKeyName()));
        long reversedTimestamp = Long.MAX_VALUE - timestamp;
        String key = longbowKey + Constants.LONGBOW_DELIMITER + reversedTimestamp;
        return Bytes.toBytes(key);
    }

    /**
     * Gets column size.
     *
     * @return the column size
     */
    public Integer getColumnSize() {
        return columnIndexMap.size();
    }

    /**
     * Gets index of a column.
     *
     * @param column the column
     * @return the index
     */
    public Integer getIndex(String column) {
        return columnIndexMap.get(column);
    }

    /**
     * Gets value on the specified column.
     *
     * @param input  the input
     * @param column the column
     * @return the value
     */
    public Object getValue(Row input, String column) {
        return input.getField(columnIndexMap.get(column));
    }

    /**
     * Check if it contains the specified column name.
     *
     * @param column the column
     * @return the boolean
     */
    public Boolean contains(String column) {
        return columnNames.contains(column);
    }

    /**
     * Gets column names.
     *
     * @param filterCondition the filter condition
     * @return the column names
     */
    public List<String> getColumnNames(Predicate<Map.Entry<String, Integer>> filterCondition) {
        return columnIndexMap
                .entrySet()
                .stream()
                .filter(filterCondition)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    /**
     * Gets column names.
     *
     * @return the column names
     */
    public List<String> getColumnNames() {
        return this.columnNames;
    }

    /**
     * Gets duration in millis.
     *
     * @param input the input
     * @return the duration in millis
     */
    public long getDurationInMillis(Row input) {
        String longbowDuration = (String) input.getField(columnIndexMap.get(Constants.LONGBOW_DURATION_KEY));
        return getDurationInMillis(longbowDuration);
    }

    /**
     * Gets duration in millis with specified duration.
     *
     * @param durationString the duration string
     * @return the duration in millis
     */
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
            default:
                throw new InvalidLongbowDurationException(String.format("'%s' is a invalid duration string", durationString));
        }
    }

    /**
     * Gets longbow type.
     *
     * @return the type
     */
    public LongbowType getType() {
        if (columnNames.contains(LongbowType.LongbowProcess.getKeyName())) {
            return LongbowType.LongbowProcess;
        } else if (columnNames.contains(LongbowType.LongbowWrite.getKeyName())) {
            return LongbowType.LongbowWrite;
        } else if (columnNames.contains(LongbowType.LongbowRead.getKeyName())) {
            return LongbowType.LongbowRead;
        }
        throw new DaggerConfigurationException("Unable to identify LongbowProcessor. Provide either "
                + LongbowType.LongbowProcess.getKeyName() + ", " + LongbowType.LongbowRead.getKeyName() + " or " + LongbowType.LongbowWrite.getKeyName());
    }

    /**
     * Check if it is a Longbow plus.
     *
     * @return the boolean
     */
    public boolean isLongbowPlus() {
        return getType() != LongbowType.LongbowProcess;
    }

    private Timestamp convertToTimeStamp(Object timeStampField) {
        if (timeStampField instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) timeStampField);
        }
        return (Timestamp) timeStampField;
    }
}
