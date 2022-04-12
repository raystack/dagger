package io.odpf.dagger.functions.udfs.python;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import io.odpf.dagger.common.configuration.Configuration;
import lombok.Getter;

import java.util.Map;

import static io.odpf.dagger.functions.common.Constants.*;

public class PythonUdfConfig {
    private static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setPrettyPrinting()
            .create();


    @SerializedName(PYTHON_FILES_KEY)
    @Getter
    private String pythonFiles;

    @SerializedName(PYTHON_REQUIREMENTS_KEY)
    @Getter
    private String pythonRequirements;

    @SerializedName(PYTHON_ARCHIVES_KEY)
    @Getter
    private String pythonArchives;

    @SerializedName(PYTHON_CLIENT_EXECUTABLE_KEY)
    @Getter
    private String pythonClientExecutable;

    @SerializedName(PYTHON_EXECUTABLE_KEY)
    @Getter
    private String pythonExecutable;

    @SerializedName(PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY)
    @Getter
    private String pythonArrowBatchSize;

    @SerializedName(PYTHON_FN_EXECUTION_BUNDLE_SIZE_KEY)
    @Getter
    private String pythonBundleSize;

    @SerializedName(PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY)
    @Getter
    private String pythonBundleTime;

    @SerializedName(PYTHON_FN_EXECUTION_MEMORY_MANAGED_KEY)
    @Getter
    private String pythonMemoryManaged;

    @SerializedName(PYTHON_MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE_KEY)
    @Getter
    private String pythonResponseBatchSize;

    @SerializedName(PYTHON_MAP_STATE_READ_CACHE_SIZE_KEY)
    @Getter
    private String pythonReadCacheSize;

    @SerializedName(PYTHON_MAP_STATE_WRITE_CACHE_SIZE_KEY)
    @Getter
    private String pythonWriteCacheSize;

    @SerializedName(PYTHON_STATE_CACHE_SIZE_KEY)
    @Getter
    private String pythonStateCacheSize;

    @SerializedName(PYTHON_METRIC_ENABLED_KEY)
    @Getter
    private String pythonMetricEnabled;

    @SerializedName(PYTHON_PROFILE_ENABLED_KEY)
    @Getter
    private String pythonProfileEnabled;

    public static PythonUdfConfig parse(Configuration configuration) {
        String jsonString = configuration.getString(PYTHON_UDF_CONFIG, "");

        return GSON.fromJson(jsonString, PythonUdfConfig.class);
    }

    public Map<String, String> jsonToConfigMap() {
        String jsonString = GSON.toJson(this);

        return GSON.fromJson(jsonString, Map.class);
    }
}
