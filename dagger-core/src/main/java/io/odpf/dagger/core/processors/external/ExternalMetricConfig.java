package io.odpf.dagger.core.processors.external;

import org.apache.flink.configuration.Configuration;

import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.utils.Constants;

import java.io.Serializable;

public class ExternalMetricConfig implements Serializable {
    private final long shutDownPeriod;
    private final boolean telemetryEnabled;
    private TelemetrySubscriber telemetrySubscriber;
    private String metricId;

    public ExternalMetricConfig(Configuration configuration, TelemetrySubscriber telemetrySubscriber) {
        this.shutDownPeriod = configuration.getLong(Constants.SHUTDOWN_PERIOD_KEY, Constants.SHUTDOWN_PERIOD_DEFAULT);
        this.telemetryEnabled = configuration.getBoolean(Constants.TELEMETRY_ENABLED_KEY, Constants.TELEMETRY_ENABLED_VALUE_DEFAULT);
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
