package io.odpf.dagger.processors.internal.processor;

import io.odpf.dagger.processors.common.RowManager;

public interface InternalConfigProcessor {
    boolean canProcess(String type);

    void process(RowManager rowManager);
}
