package io.odpf.dagger.common.core;

public class Constants {
    public static final String STENCIL_ENABLE_KEY = "ENABLE_STENCIL_URL";
    public static final boolean STENCIL_ENABLE_DEFAULT = false;
    public static final String STENCIL_URL_KEY = "STENCIL_URL";
    public static final String STENCIL_URL_DEFAULT = "";

    public static final String STENCIL_CONFIG_TTL_IN_MINUTES_KEY = "TIL_IN_MINUTES";
    public static final String STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT = "0";
    public static final String STENCIL_CONFIG_TIMEOUT_MS_KEY = "STENCIL_TIMEOUT_MS";
    public static final String STENCIL_CONFIG_TIMEOUT_MS_DEFAULT = "60000";
    public static final String STENCIL_CONFIG_REFRESH_CACHE_KEY = "REFRESH_CACHE";
    public static final String STENCIL_CONFIG_REFRESH_CACHE_DEFAULT = "false";

    public static final String UDF_TELEMETRY_GROUP_KEY = "udf";
    public static final String GAUGE_ASPECT_NAME = "value";

    public static final long SLIDING_TIME_WINDOW = 10;
}
