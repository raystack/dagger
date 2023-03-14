package com.gotocompany.dagger.core.exception;

/***
 * This exception is thrown when the reader for Parquet FileSource could not be initialized.
 */
public class ParquetFileSourceReaderInitializationException extends RuntimeException {
    public ParquetFileSourceReaderInitializationException(Throwable cause) {
        super(cause);
    }
}
