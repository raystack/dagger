package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;

import static com.gojek.daggers.utils.Constants.LONGBOW_DATA;

public class LongbowDataFactory {
    private LongbowSchema longbowSchema;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private String protoClassName;

    public LongbowDataFactory(LongbowSchema longbowSchema, StencilClientOrchestrator stencilClientOrchestrator, String protoClassName) {
        this.longbowSchema = longbowSchema;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.protoClassName = protoClassName;
    }

    public LongbowData getLongbowData() {
        if (longbowSchema.contains(LONGBOW_DATA)) {
            return new LongbowTableData(longbowSchema);
        }
        return new LongbowProtoData(stencilClientOrchestrator, protoClassName);
    }
}
