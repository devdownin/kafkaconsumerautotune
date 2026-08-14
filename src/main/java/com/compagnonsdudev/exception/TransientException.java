package com.compagnonsdudev.exception;

/**
 * Exception indicating a temporary failure that may succeed if retried.
 * Examples include database timeouts or temporary network unavailability.
 */
public class TransientException extends RuntimeException {
    /**
     * Constructs a new TransientException with the specified message.
     *
     * @param message The detail message.
     */
    public TransientException(String message) {
        super(message);
    }

    /**
     * Constructs a new TransientException with the specified message and cause.
     *
     * @param message The detail message.
     * @param cause The cause of the exception.
     */
    public TransientException(String message, Throwable cause) {
        super(message, cause);
    }
}
