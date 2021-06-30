package io.odpf.dagger.functions.exceptions;

/**
 * The Exception class for ArrayAggregate udf.
 */
public class ArrayAggregationException extends RuntimeException {
    /**
     * Instantiates a new Array aggregation exception.
     *
     * @param message the message
     */
    public ArrayAggregationException(String message) {
        super(message);
    }

    /**
     * Instantiates a new Array aggregation exception with innerException.
     *
     * @param innerException the inner exception
     */
    public ArrayAggregationException(Exception innerException) {
        super(innerException);
    }
}
