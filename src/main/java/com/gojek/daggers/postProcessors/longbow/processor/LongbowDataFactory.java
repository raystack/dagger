package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;

public class LongbowDataFactory {
    private LongbowSchema longbowSchema;

    public LongbowDataFactory(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    public LongbowData getLongbowData() {
        if (longbowSchema.hasLongbowData()) {
            return new LongbowTableData(longbowSchema);
        }
        return new LongbowProtoData();
    }
}
