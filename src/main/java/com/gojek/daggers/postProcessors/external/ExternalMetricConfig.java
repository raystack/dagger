package com.gojek.daggers.postProcessors.external;

import org.apache.flink.configuration.Configuration;

import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;

import java.io.Serializable;

import static com.gojek.daggers.utils.Constants.SHUTDOWN_PERIOD_DEFAULT;
import static com.gojek.daggers.utils.Constants.SHUTDOWN_PERIOD_KEY;
import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_KEY;
import static com.gojek.daggers.utils.Constants.TELEMETRY_ENABLED_VALUE_DEFAULT;

public class ExternalMetricConfig implements Serializable {
    private final long shutDownPeriod;
    private final boolean telemetryEnabled;
    private TelemetrySubscriber telemetrySubscriber;
    private String metricId;

    public ExternalMetricConfig(Configuration configuration, TelemetrySubscriber telemetrySubscriber) {
        this.shutDownPeriod = configuration.getLong(SHUTDOWN_PERIOD_KEY, SHUTDOWN_PERIOD_DEFAULT);
        this.telemetryEnabled = configuration.getBoolean(TELEMETRY_ENABLED_KEY, TELEMETRY_ENABLED_VALUE_DEFAULT);
        this.telemetrySubscriber = telemetrySubscriber;
    }

    public ExternalMetricConfig(String metricId, long shutDownPeriod, boolean telemetryEnabled) {
        this.metricId = metricId;
        this.shutDownPeriod = shutDownPeriod;
        this.telemetryEnabled = telemetryEnabled;
    }

    public String getMetricId() {
        return metricId;
    }

    public void setMetricId(String metricId) {
        this.metricId = metricId;
    }

    public TelemetrySubscriber getTelemetrySubscriber() {
        return telemetrySubscriber;
    }

    public boolean isTelemetryEnabled() {
        return telemetryEnabled;
    }

    public long getShutDownPeriod() {
        return shutDownPeriod;
    }
}
