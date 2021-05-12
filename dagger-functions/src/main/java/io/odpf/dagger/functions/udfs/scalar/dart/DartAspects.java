package io.odpf.dagger.functions.udfs.scalar.dart;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

import static io.odpf.dagger.common.metrics.aspects.AspectType.Gauge;
import static io.odpf.dagger.common.metrics.aspects.AspectType.Metric;

public enum DartAspects implements Aspects {

    DART_GCS_PATH("DartBucketPath", Gauge),
    DART_GCS_FETCH_FAILURES("DartGcsBucketFetchFailure", Metric),
    DART_GCS_FETCH_SUCCESS("DartGcsBucketFetchSuccess", Metric),
    DART_CACHE_HIT("DartCacheFetchSuccess", Metric),
    DART_CACHE_MISS("DartCacheFetchFailure", Metric),
    DART_GCS_FILE_SIZE("DartGcsFileSize", Gauge);

    private String value;
    private AspectType aspectType;

    DartAspects(String value, AspectType aspectType) {
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
