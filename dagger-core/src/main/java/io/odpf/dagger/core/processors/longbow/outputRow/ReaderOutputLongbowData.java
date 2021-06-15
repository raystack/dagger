package io.odpf.dagger.core.processors.longbow.outputRow;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.utils.Constants;

import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * The Reader output longbow data.
 */
public class ReaderOutputLongbowData implements ReaderOutputRow {
    private LongbowSchema longbowSchema;

    /**
     * Instantiates a new Reader output longbow data.
     *
     * @param longbowSchema the longbow schema
     */
    public ReaderOutputLongbowData(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public Row get(Map<String, Object> scanResult, Row input) {
        HashMap<String, Object> columnMap = new HashMap<>();
        longbowSchema.getColumnNames(c -> !isLongbowData(c))
                .forEach(name -> columnMap.put(name, longbowSchema.getValue(input, name)));
        scanResult.forEach(columnMap::put);
        int arity = input.getArity();
        Row output = new Row(arity);
        columnMap.forEach((name, data) -> {
            output.setField(longbowSchema.getIndex(name), data);
        });
        return output;
    }

    private boolean isLongbowData(Map.Entry<String, Integer> c) {
        return c.getKey().contains(Constants.LONGBOW_DATA_KEY);
    }
}
