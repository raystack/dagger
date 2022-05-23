package io.odpf.dagger.functions.udfs.python;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import io.odpf.dagger.common.configuration.Configuration;
import lombok.Getter;

import static io.odpf.dagger.functions.common.Constants.*;

/**
 * The type Python udf config.
 */
public class PythonUdfConfig {
    private static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setPrettyPrinting()
            .create();

    @SerializedName(PYTHON_FILES_KEY)
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

    /**
     * Gets python files.
     *
     * @return the python files
     */
    public String getPythonFiles() {
        if (pythonFiles != null) {
            return pythonFiles.replaceAll("\\s+", "");
        }
        return null;
    }

    /**
     * Gets python arrow batch size.
     *
     * @return the python arrow batch size
     */
    public int getPythonArrowBatchSize() {
        if (pythonArrowBatchSize == null) {
            return PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_DEFAULT;
        }
        return pythonArrowBatchSize;
    }

    /**
     * Gets python bundle size.
     *
     * @return the python bundle size
     */
    public int getPythonBundleSize() {
        if (pythonBundleSize == null) {
            return PYTHON_FN_EXECUTION_BUNDLE_SIZE_DEFAULT;
        }
        return pythonBundleSize;
    }

    /**
     * Gets python bundle time.
     *
     * @return the python bundle time
     */
    public long getPythonBundleTime() {
        if (pythonBundleTime == null) {
            return PYTHON_FN_EXECUTION_BUNDLE_TIME_DEFAULT;
        }
        return pythonBundleTime;
    }

    /**
     * Parse python udf config.
     *
     * @param configuration the configuration
     * @return the python udf config
     */
    public static PythonUdfConfig parse(Configuration configuration) {
        String jsonString = configuration.getString(PYTHON_UDF_CONFIG, "");

        return GSON.fromJson(jsonString, PythonUdfConfig.class);
    }
}
