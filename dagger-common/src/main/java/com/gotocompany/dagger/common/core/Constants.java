package com.gotocompany.dagger.common.core;

public class Constants {
    public static final String SCHEMA_REGISTRY_STENCIL_ENABLE_KEY = "SCHEMA_REGISTRY_STENCIL_ENABLE";
    public static final boolean SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT = false;
    public static final String SCHEMA_REGISTRY_STENCIL_URLS_KEY = "SCHEMA_REGISTRY_STENCIL_URLS";
    public static final String SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT = "";
    public static final String SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS = "SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS";
    public static final Integer SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT = 10000;
    public static final String SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS_KEY = "SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS";
    public static final String SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS_DEFAULT = "";
    public static final String SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY = "SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH";
    public static final boolean SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT = false;
    public static final String SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_KEY = "SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS";
    public static final Long SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT = 900000L;
    public static final String SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY = "SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY";
    public static final String SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT = "LONG_POLLING";
    public static final String SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS_KEY = "SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS";
    public static final Long SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS_DEFAULT = 60000L;
    public static final String SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES_KEY = "SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES";
    public static final Integer SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES_DEFAULT = 4;

    public static final String UDF_TELEMETRY_GROUP_KEY = "udf";
    public static final String GAUGE_ASPECT_NAME = "value";

    public static final long SLIDING_TIME_WINDOW = 10;
    public static final String STREAM_INPUT_SCHEMA_PROTO_CLASS = "INPUT_SCHEMA_PROTO_CLASS";
    public static final String STREAM_INPUT_SCHEMA_TABLE = "INPUT_SCHEMA_TABLE";
    public static final String INPUT_STREAMS = "STREAMS";

    public static final String INTERNAL_VALIDATION_FIELD_KEY = "__internal_validation_field__";
    public static final String ROWTIME = "rowtime";
}
