package io.odpf.dagger.functions.udfs.scalar.dart;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

import static io.odpf.dagger.common.metrics.aspects.AspectType.Gauge;
import static io.odpf.dagger.common.metrics.aspects.AspectType.Metric;

/**
 * The enum Dart aspects.
 */
public enum DartAspects implements Aspects {

    /**
     * Dart gcs path dart aspects.
     */
    DART_GCS_PATH("dart_bucket_path", Gauge),
    /**
     * Dart gcs fetch failures dart aspects.
     */
    DART_GCS_FETCH_FAILURES("dart_gcs_bucket_fetch_failure", Metric),
    /**
     * Dart gcs fetch success dart aspects.
     */
    DART_GCS_FETCH_SUCCESS("dart_gcs_bucket_fetch_success", Metric),
    /**
     * Dart cache hit dart aspects.
     */
    DART_CACHE_HIT("dart_cache_fetch_success", Metric),
    /**
     * Dart cache miss dart aspects.
     */
    DART_CACHE_MISS("dart_cache_fetch_failure", Metric),
    /**
     * Dart gcs file size dart aspects.
     */
    DART_GCS_FILE_SIZE("dart_gcs_file_size", Gauge);

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
