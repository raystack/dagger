package com.gojek.daggers.async.metric;

import com.gojek.daggers.utils.stats.AspectType;
import com.gojek.daggers.utils.stats.Aspects;

import static com.gojek.daggers.utils.stats.AspectType.Metric;

public enum ExternalSourceAspects implements Aspects {
    CLOSE_CONNECTION_ON_HTTP_CLIENT("closeConnectionOnHttpClient", Metric),
    TOTAL_HTTP_CALLS("totalHttpCalls", Metric),
    TIMEOUTS("timeouts", Metric);

    private String value;
    private AspectType aspectType;

    ExternalSourceAspects(String value, AspectType aspectType) {
        this.value = value;
        this.aspectType = aspectType;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public AspectType getAspectType() {
        return aspectType;
    }
}
