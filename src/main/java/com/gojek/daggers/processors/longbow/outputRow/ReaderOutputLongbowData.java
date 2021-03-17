package com.gojek.daggers.processors.longbow.outputRow;

import com.gojek.daggers.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

import static com.gojek.daggers.utils.Constants.LONGBOW_DATA;

public class ReaderOutputLongbowData implements ReaderOutputRow {
    private LongbowSchema longbowSchema;

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
        return c.getKey().contains(LONGBOW_DATA);
    }
}
