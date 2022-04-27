package io.odpf.dagger.functions.udfs.python;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import io.odpf.dagger.common.configuration.Configuration;
import lombok.Getter;

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

    @SerializedName(PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY)
    private Integer pythonArrowBatchSize;

    @SerializedName(PYTHON_FN_EXECUTION_BUNDLE_SIZE_KEY)
    private Integer pythonBundleSize;

    @SerializedName(PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY)
    private Long pythonBundleTime;

    public int getPythonArrowBatchSize() {
        if (pythonArrowBatchSize == null) {
            return PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_DEFAULT;
        }
        return pythonArrowBatchSize;
    }

    public int getPythonBundleSize() {
        if (pythonBundleSize == null) {
            return PYTHON_FN_EXECUTION_BUNDLE_SIZE_DEFAULT;
        }
        return pythonBundleSize;
    }

    public long getPythonBundleTime() {
        if (pythonBundleTime == null) {
            return PYTHON_FN_EXECUTION_BUNDLE_TIME_DEFAULT;
        }
        return pythonBundleTime;
    }

    public static PythonUdfConfig parse(Configuration configuration) {
        String jsonString = configuration.getString(PYTHON_UDF_CONFIG, "");

        return GSON.fromJson(jsonString, PythonUdfConfig.class);
    }
}
