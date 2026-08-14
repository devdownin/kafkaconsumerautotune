package com.compagnonsdudev.exception;

/**
 * Exception indicating a permanent failure that should not be retried.
 * Examples include invalid data formats or business rule violations.
 */
public class PermanentException extends RuntimeException {
    /**
     * Constructs a new PermanentException with the specified message.
     *
     * @param message The detail message.
     */
    public PermanentException(String message) {
        super(message);
    }

    /**
     * Constructs a new PermanentException with the specified message and cause.
     *
     * @param message The detail message.
     * @param cause The cause of the exception.
     */
    public PermanentException(String message, Throwable cause) {
        super(message, cause);
    }
}
