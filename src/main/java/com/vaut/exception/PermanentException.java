package com.vaut.exception;

public class PermanentException extends RuntimeException {
    public PermanentException(String message) {
        super(message);
    }

    public PermanentException(String message, Throwable cause) {
        super(message, cause);
    }
}
