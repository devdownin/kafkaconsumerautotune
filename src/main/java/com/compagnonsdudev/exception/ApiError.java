package com.compagnonsdudev.exception;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.Map;

/**
 * Error payload returned to API clients.
 *
 * <p>Mirrors the fields the HTML error page already shows, so both surfaces
 * describe a failure the same way.
 *
 * @param timestamp When the failure was handled.
 * @param status The HTTP status code, repeated in the body for clients that log it.
 * @param error The short reason phrase for that status.
 * @param message A human-readable description of the failure.
 * @param path The request URI that failed.
 * @param fieldErrors Field name to violation message; empty unless the request failed validation.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record ApiError(
        Instant timestamp,
        int status,
        String error,
        String message,
        String path,
        Map<String, String> fieldErrors
) {

    /**
     * Builds an error payload, filling in the timestamp and the status metadata.
     *
     * @param status The HTTP status to report.
     * @param message A human-readable description of the failure.
     * @param path The request URI that failed.
     * @param fieldErrors Field violations, or an empty map when not a validation failure.
     */
    public ApiError(HttpStatus status, String message, String path, Map<String, String> fieldErrors) {
        this(Instant.now(), status.value(), status.getReasonPhrase(), message, path, fieldErrors);
    }
}
