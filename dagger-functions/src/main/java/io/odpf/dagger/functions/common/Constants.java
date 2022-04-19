package io.odpf.dagger.functions.common;

public class Constants {
    public static final Integer NUMBER_OF_DATA_TYPES_IN_FEATURE_ROW = 8;
    public static final Integer NUMBER_OF_ARGUMENTS_IN_FEATURE_ACCUMULATOR = 3;
    public static final String UDF_DART_GCS_PROJECT_ID_KEY = "UDF_DART_GCS_PROJECT_ID";
    public static final String UDF_DART_GCS_PROJECT_ID_DEFAULT = "";
    public static final String UDF_DART_GCS_BUCKET_ID_KEY = "UDF_DART_GCS_BUCKET_ID";
    public static final String UDF_DART_GCS_BUCKET_ID_DEFAULT = "";

    public static final String PYTHON_UDF_CONFIG = "PYTHON_UDF_CONFIG";
    public static final String PYTHON_UDF_ENABLE_KEY = "PYTHON_UDF_ENABLE";
    public static final boolean PYTHON_UDF_ENABLE_DEFAULT = false;
    public static final String PYTHON_FILES_KEY = "PYTHON_FILES";
    public static final String PYTHON_REQUIREMENTS_KEY = "PYTHON_REQUIREMENTS";
    public static final String PYTHON_ARCHIVES_KEY = "PYTHON_ARCHIVES";
    public static final String PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY = "PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE";
    public static final Integer PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_DEFAULT = 10000;
    public static final String PYTHON_FN_EXECUTION_BUNDLE_SIZE_KEY = "PYTHON_FN_EXECUTION_BUNDLE_SIZE";
    public static final Integer PYTHON_FN_EXECUTION_BUNDLE_SIZE_DEFAULT = 100000;
    public static final String PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY = "PYTHON_FN_EXECUTION_BUNDLE_TIME";
    public static final long PYTHON_FN_EXECUTION_BUNDLE_TIME_DEFAULT = 1000;
}
