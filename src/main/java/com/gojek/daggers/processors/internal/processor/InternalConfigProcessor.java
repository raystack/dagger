package com.gojek.daggers.processors.internal.processor;

import com.gojek.daggers.processors.common.RowManager;

public interface InternalConfigProcessor {
    boolean canProcess(String type);

    void process(RowManager rowManager);
}
