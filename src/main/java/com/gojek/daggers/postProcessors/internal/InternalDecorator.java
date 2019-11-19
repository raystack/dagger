package com.gojek.daggers.postProcessors.internal;

import com.gojek.daggers.postProcessors.external.common.MapDecorator;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.internal.processor.InternalConfigProcessor;
import org.apache.flink.types.Row;

public class InternalDecorator implements MapDecorator {

    private InternalSourceConfig internalSourceConfig;
    private InternalConfigProcessor internalConfigProcessor;

    public InternalDecorator(InternalSourceConfig internalSourceConfig, InternalConfigProcessor internalConfigProcessor) {
        this.internalSourceConfig = internalSourceConfig;
        this.internalConfigProcessor = internalConfigProcessor;
    }

    @Override
    public Boolean canDecorate() {
        return internalSourceConfig != null;
    }

    @Override
    public Row map(Row input) {
        RowManager rowManager = new RowManager(input);
        internalConfigProcessor.process(rowManager);
        return rowManager.getAll();
    }


}
