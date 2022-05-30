package io.odpf.dagger.core.source.config;

import com.google.common.base.Preconditions;
import io.odpf.dagger.core.source.config.models.SourceDetails;
import io.odpf.dagger.core.source.config.models.SourceName;

import java.util.Arrays;
import java.util.stream.Stream;

import static io.odpf.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_KEY;
import static io.odpf.dagger.core.utils.Constants.STREAM_SOURCE_PARQUET_FILE_PATHS_KEY;

public class StreamConfigValidator {
    public static StreamConfig validateSourceDetails(StreamConfig streamConfig) {
        SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
        for (SourceDetails sourceDetails : sourceDetailsArray) {
            Preconditions.checkArgument(sourceDetails != null, "One or more elements inside %s "
                    + "is either null or invalid.", STREAM_SOURCE_DETAILS_KEY);
            Preconditions.checkArgument(sourceDetails.getSourceName() != null, "One or more "
                    + "elements inside %s has null or invalid SourceName. Check if it is a valid SourceName and ensure "
                    + "no trailing/leading whitespaces are present", STREAM_SOURCE_DETAILS_KEY);
            Preconditions.checkArgument(sourceDetails.getSourceType() != null, "One or more "
                    + "elements inside %s has null or invalid SourceType. Check if it is a valid SourceType and ensure "
                    + "no trailing/leading whitespaces are present", STREAM_SOURCE_DETAILS_KEY);
        }
        return streamConfig;
    }

    public static StreamConfig validateParquetDataSourceStreamConfigs(StreamConfig streamConfig) {
        SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
        for (SourceDetails sourceDetails : sourceDetailsArray) {
            if (sourceDetails.getSourceName().equals(SourceName.PARQUET_SOURCE)) {
                return Stream.of(streamConfig)
                        .map(StreamConfigValidator::validateParquetFilePaths)
                        .findFirst()
                        .get();
            }
        }
        return streamConfig;
    }

    private static StreamConfig validateParquetFilePaths(StreamConfig streamConfig) {
        String[] parquetFilePaths = streamConfig.getParquetFilePaths();
        Preconditions.checkArgument(parquetFilePaths != null, "%s is required for configuring a Parquet Data Source Stream, but is set to null.", STREAM_SOURCE_PARQUET_FILE_PATHS_KEY);
        Arrays.stream(parquetFilePaths)
                .forEach(filePath -> Preconditions.checkArgument(!filePath.equals("null"),
                        "One or more file path inside %s is null.", STREAM_SOURCE_PARQUET_FILE_PATHS_KEY));
        return streamConfig;
    }
}
