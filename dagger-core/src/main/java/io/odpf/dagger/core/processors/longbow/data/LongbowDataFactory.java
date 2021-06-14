package io.odpf.dagger.core.processors.longbow.data;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;

/**
 * The factory class for Longbow data.
 */
public class LongbowDataFactory {
    private LongbowSchema longbowSchema;

    /**
     * Instantiates a new Longbow data factory.
     *
     * @param longbowSchema the longbow schema
     */
    public LongbowDataFactory(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    /**
     * Gets longbow data.
     *
     * @return the longbow data
     */
    public LongbowData getLongbowData() {
        if (!longbowSchema.isLongbowPlus()) {
            return new LongbowTableData(longbowSchema);
        }
        return new LongbowProtoData();
    }
}
