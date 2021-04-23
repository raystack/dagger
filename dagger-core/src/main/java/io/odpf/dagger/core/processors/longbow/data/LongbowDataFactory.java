package io.odpf.dagger.core.processors.longbow.data;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;

public class LongbowDataFactory {
    private LongbowSchema longbowSchema;

    public LongbowDataFactory(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    public LongbowData getLongbowData() {
        if (!longbowSchema.isLongbowPlus()) {
            return new LongbowTableData(longbowSchema);
        }
        return new LongbowProtoData();
    }
}
