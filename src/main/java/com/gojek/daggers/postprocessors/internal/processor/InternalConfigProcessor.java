package com.gojek.daggers.postprocessors.internal.processor;

import com.gojek.daggers.postprocessors.external.common.RowManager;

public interface InternalConfigProcessor {
    boolean canProcess(String type);

    void process(RowManager rowManager);
}
