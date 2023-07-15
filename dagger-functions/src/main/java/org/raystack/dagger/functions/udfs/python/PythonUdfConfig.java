package org.raystack.dagger.functions.udfs.python;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.functions.common.Constants;
import lombok.Getter;

/**
 * The type Python udf config.
 */
public class PythonUdfConfig {
    private static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setPrettyPrinting()
            .create();

    @SerializedName(Constants.PYTHON_FILES_KEY)
    private String pythonFiles;

    @SerializedName(Constants.PYTHON_REQUIREMENTS_KEY)
    @Getter
    private String pythonRequirements;

    @SerializedName(Constants.PYTHON_ARCHIVES_KEY)
    private String pythonArchives;

    @SerializedName(Constants.PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_KEY)
    private Integer pythonArrowBatchSize;

    @SerializedName(Constants.PYTHON_FN_EXECUTION_BUNDLE_SIZE_KEY)
    private Integer pythonBundleSize;

    @SerializedName(Constants.PYTHON_FN_EXECUTION_BUNDLE_TIME_KEY)
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
     * Gets python archives.
     *
     * @return the python archives
     */
    public String getPythonArchives() {
        if (pythonArchives != null) {
            return pythonArchives.replaceAll("\\s+", "");
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
            return Constants.PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE_DEFAULT;
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
            return Constants.PYTHON_FN_EXECUTION_BUNDLE_SIZE_DEFAULT;
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
            return Constants.PYTHON_FN_EXECUTION_BUNDLE_TIME_DEFAULT;
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
        String jsonString = configuration.getString(Constants.PYTHON_UDF_CONFIG, "");

        return GSON.fromJson(jsonString, PythonUdfConfig.class);
    }
}
