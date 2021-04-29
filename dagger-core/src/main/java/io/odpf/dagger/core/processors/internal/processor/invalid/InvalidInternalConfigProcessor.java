package io.odpf.dagger.core.processors.internal.processor.invalid;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.InternalConfigProcessor;
import org.apache.commons.lang3.StringUtils;

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
        if (internalSourceConfig != null && StringUtils.isNotEmpty(internalSourceConfig.getType()))
            type = internalSourceConfig.getType();
        throw new InvalidConfigurationException(String.format("Invalid configuration, type '%s' for custom doesn't exists", type));
    }
}
