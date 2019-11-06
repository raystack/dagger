package com.gojek.daggers.postProcessors.internal.processor.invalid;

import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;
import com.gojek.daggers.postProcessors.internal.processor.InternalConfigProcessor;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

public class InvalidInternalConfigProcessor implements InternalConfigProcessor, Serializable {

    private InternalSourceConfig internalSourceConfig;

    public InvalidInternalConfigProcessor(InternalSourceConfig internalSourceConfig) {
        this.internalSourceConfig = internalSourceConfig;
    }

    @Override
    public boolean canProcess(String type) {
        return false;
    }

    public void process(RowManager rowManager) {
        String type = "";
        if(internalSourceConfig!=null && StringUtils.isNotEmpty(internalSourceConfig.getType()))
            type = internalSourceConfig.getType();
        throw new InvalidConfigurationException(String.format("Invalid configuration, type '%s' for custom doesn't exists", type));
    }
}
