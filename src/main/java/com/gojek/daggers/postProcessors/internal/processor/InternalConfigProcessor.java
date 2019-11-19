package com.gojek.daggers.postProcessors.internal.processor;

import com.gojek.daggers.postProcessors.external.common.RowManager;

public interface InternalConfigProcessor {
    boolean canProcess(String type);

    void process(RowManager rowManager);
}
