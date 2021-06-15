package io.odpf.dagger.core.processors.longbow.outputRow;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.utils.Constants;

import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * The Reader output proto data.
 */
public class ReaderOutputProtoData implements ReaderOutputRow {
    private LongbowSchema longbowSchema;

    /**
     * Instantiates a new Reader output proto data.
     *
     * @param longbowSchema the longbow schema
     */
    public ReaderOutputProtoData(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public Row get(Map<String, Object> scanResult, Row input) {
        HashMap<String, Object> columnMap = new HashMap<>();
        longbowSchema.getColumnNames(c -> !isLongbowProtoData(c))
                .forEach(name -> columnMap.put(name, longbowSchema.getValue(input, name)));
        int arity = columnMap.size() + 1;
        Row output = new Row(arity);
        columnMap.forEach((name, data) -> {
            output.setField(longbowSchema.getIndex(name), data);
        });
        output.setField(arity - 1, scanResult.get(Constants.LONGBOW_PROTO_DATA_KEY));
        return output;
    }

    private boolean isLongbowProtoData(Map.Entry<String, Integer> c) {
        return c.getKey().contains(Constants.LONGBOW_PROTO_DATA_KEY);
    }
}
