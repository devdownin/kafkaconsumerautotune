package com.compagnonsdudev.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.ModelAndView;

import jakarta.servlet.http.HttpServletRequest;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Global exception handler for both the web UI and the REST endpoints.
 *
 * <p>Browsers get the friendly error page instead of the default Whitelabel one;
 * API clients get a JSON body they can act on. Both get a status code that
 * reflects the failure — previously every handled exception was answered with
 * 200 OK, which hid outages from health checks and made failed API calls look
 * successful to the dashboard's own JavaScript.
 */
@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * Rejects a request whose body failed bean validation.
     *
     * <p>Only reachable from the JSON endpoints, which are the only ones binding
     * a {@code @Valid @RequestBody}.
     *
     * @param ex The validation failure raised during argument binding.
     * @param request The request that failed validation.
     * @return A 400 response listing the offending fields.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidationException(MethodArgumentNotValidException ex,
                                                             HttpServletRequest request) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> fieldErrors.putIfAbsent(error.getField(), error.getDefaultMessage()));

        log.warn("Rejected invalid request to {}: {}", request.getRequestURI(), fieldErrors);

        return ResponseEntity.badRequest().body(new ApiError(
                HttpStatus.BAD_REQUEST,
                "The request parameters are invalid.",
                request.getRequestURI(),
                fieldErrors));
    }

    /**
     * Handles any exception no other handler claimed.
     *
     * <p>The declared return type is {@code Object} on purpose: Spring resolves
     * the return value against its runtime class, so the same handler can answer
     * a browser with a rendered view and an API client with a JSON body.
     *
     * @param ex The unhandled exception.
     * @param request The request that failed.
     * @return A {@link ResponseEntity} carrying an {@link ApiError} for API
     *         clients, or a {@link ModelAndView} rendering the error page.
     */
    @ExceptionHandler(Exception.class)
    public Object handleException(Exception ex, HttpServletRequest request) {
        log.error("Unhandled exception occurred during request to {}: {}", request.getRequestURI(), ex.getMessage(), ex);

        if (expectsJson(request)) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ApiError(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    ex.getMessage(),
                    request.getRequestURI(),
                    Map.of()));
        }

        ModelAndView errorView = new ModelAndView("error");
        errorView.setStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        errorView.addObject("error", ex.getClass().getSimpleName());
        errorView.addObject("message", ex.getMessage());
        errorView.addObject("path", request.getRequestURI());
        return errorView;
    }

    /**
     * Decides whether the caller wants a machine-readable answer.
     *
     * <p>Path first, because every REST controller is mapped under {@code /api/};
     * then the negotiated media type, which covers the handful of JSON endpoints
     * living outside that prefix (the DLT retry-with-payload call).
     *
     * @param request The request being answered.
     * @return true when the response should be JSON rather than an HTML page.
     */
    private boolean expectsJson(HttpServletRequest request) {
        String uri = request.getRequestURI();
        if (uri != null && uri.startsWith("/api/")) {
            return true;
        }
        String accept = request.getHeader(HttpHeaders.ACCEPT);
        if (accept != null && accept.contains(MediaType.APPLICATION_JSON_VALUE)) {
            return true;
        }
        return "XMLHttpRequest".equals(request.getHeader("X-Requested-With"));
    }
}
