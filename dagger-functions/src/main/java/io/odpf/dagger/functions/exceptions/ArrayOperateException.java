package io.odpf.dagger.functions.exceptions;

/**
 * The Exception class for ArrayOperate udf.
 */
public class ArrayOperateException extends RuntimeException {

    /**
     * Instantiates a new Array operate exception.
     *
     * @param message the message
     */
    public ArrayOperateException(String message) {
        super(message);
    }

    /**
     * Instantiates a new Array operate exception.
     *
     * @param innerException the inner exception
     */
    public ArrayOperateException(Exception innerException) {
        super(innerException);
    }
}
