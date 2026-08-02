package com.vaut.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import jakarta.servlet.http.HttpServletRequest;

import java.util.UUID;

/**
 * Global exception handler for the web UI.
 * Provides a user-friendly error page instead of the default Whitelabel Error Page.
 */
@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public String handleException(Exception ex, Model model, HttpServletRequest request) {
        // A correlation id is surfaced to the user instead of the exception text: the message can
        // carry SQL fragments, connection strings and other internals. The full detail goes to the
        // log, where the same id ties the two together.
        String errorId = UUID.randomUUID().toString();
        log.error("Unhandled exception [{}] during request to {}: {}", errorId, request.getRequestURI(), ex.getMessage(), ex);

        model.addAttribute("error", ex.getClass().getSimpleName());
        model.addAttribute("message", "An unexpected error occurred. Reference: " + errorId);
        model.addAttribute("errorId", errorId);
        model.addAttribute("path", request.getRequestURI());

        return "error";
    }
}
