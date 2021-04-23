package io.odpf.dagger.core.processors.internal.processor;

import io.odpf.dagger.core.processors.common.RowManager;

public interface InternalConfigProcessor {
    boolean canProcess(String type);

    void process(RowManager rowManager);
}
