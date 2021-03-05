package com.gojek.daggers.postprocessors.longbow.outputRow;

import com.gojek.daggers.postprocessors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

import static com.gojek.daggers.utils.Constants.LONGBOW_PROTO_DATA;

public class ReaderOutputProtoData implements ReaderOutputRow {
    private LongbowSchema longbowSchema;

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
        output.setField(arity - 1, scanResult.get(LONGBOW_PROTO_DATA));
        return output;
    }

    private boolean isLongbowProtoData(Map.Entry<String, Integer> c) {
        return c.getKey().contains(LONGBOW_PROTO_DATA);
    }
}
