package io.odpf.dagger.core.source.parquet;

import static com.google.api.client.util.Preconditions.checkArgument;

import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.metrics.reporters.statsd.StatsDErrorReporter;
import io.odpf.dagger.core.source.parquet.reader.ReaderProvider;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.util.function.Supplier;

public class ParquetFileRecordFormat implements FileRecordFormat<Row> {
    private final ReaderProvider parquetFileReaderProvider;
    private final Supplier<TypeInformation<Row>> typeInformationProvider;
    private final StatsDErrorReporter statsDErrorReporter;

    private ParquetFileRecordFormat(ReaderProvider parquetFileReaderProvider, Supplier<TypeInformation<Row>> typeInformationProvider, SerializedStatsDReporterSupplier statsDReporterSupplier) {
        this.parquetFileReaderProvider = parquetFileReaderProvider;
        this.typeInformationProvider = typeInformationProvider;
        this.statsDErrorReporter = new StatsDErrorReporter(statsDReporterSupplier);
    }

    @Override
    public Reader<Row> createReader(Configuration config, Path filePath, long splitOffset, long splitLength) {
        return parquetFileReaderProvider.getReader(filePath.toString());
    }

    @Override
    public Reader<Row> restoreReader(Configuration config, Path filePath, long restoredOffset, long splitOffset, long splitLength) {
        UnsupportedOperationException ex = new UnsupportedOperationException("Error: ParquetReader do not have offsets and hence cannot be restored "
                + "via this method.");
        statsDErrorReporter.reportFatalException(ex);
        throw ex;
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInformationProvider.get();
    }

    public static class Builder {
        private ReaderProvider parquetFileReaderProvider;
        private Supplier<TypeInformation<Row>> typeInformationProvider;
        private SerializedStatsDReporterSupplier statsDReporterSupplier;

        public static Builder getInstance() {
            return new Builder();
        }

        private Builder() {
            this.parquetFileReaderProvider = null;
            this.typeInformationProvider = null;
            this.statsDReporterSupplier = null;
        }

        public Builder setParquetFileReaderProvider(ReaderProvider parquetFileReaderProvider) {
            this.parquetFileReaderProvider = parquetFileReaderProvider;
            return this;
        }

        public Builder setTypeInformationProvider(Supplier<TypeInformation<Row>> typeInformationProvider) {
            this.typeInformationProvider = typeInformationProvider;
            return this;
        }

        public Builder setStatsDReporterSupplier(SerializedStatsDReporterSupplier statsDReporterSupplier) {
            this.statsDReporterSupplier = statsDReporterSupplier;
            return this;
        }

        public ParquetFileRecordFormat build() {
            try {
                checkArgument(parquetFileReaderProvider != null, "ReaderProvider is required but is set as null");
                checkArgument(typeInformationProvider != null, "TypeInformationProvider is required but is set as null");
                checkArgument(statsDReporterSupplier != null, "SerializedStatsDReporterSupplier is required but is set as null");
                return new ParquetFileRecordFormat(parquetFileReaderProvider, typeInformationProvider, statsDReporterSupplier);
            } catch (IllegalArgumentException ex) {
                if (statsDReporterSupplier != null) {
                    new StatsDErrorReporter(statsDReporterSupplier).reportFatalException(ex);
                }
                throw ex;
            }
        }
    }
}
