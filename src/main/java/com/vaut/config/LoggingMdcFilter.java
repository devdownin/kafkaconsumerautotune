package com.vaut.config;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.core.annotation.Order;

import java.io.IOException;
import java.util.UUID;

/**
 * Filter that populates the Mapped Diagnostic Context (MDC) with request-specific information.
 * This information is used for structured logging (ELK).
 */
@Component
@Order(1)
public class LoggingMdcFilter implements Filter {

    private static final String AUTH_HEADER = "Authorization";
    private static final String RGPD_HEADER = "X-RGPD";

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest httpServletRequest) {
            String correlationId = httpServletRequest.getHeader(AppConstants.HEADER_CORRELATION_ID);
            if (correlationId == null || correlationId.isEmpty()) {
                correlationId = UUID.randomUUID().toString();
            }
            String authHeader = httpServletRequest.getHeader(AUTH_HEADER);
            String rgpd = httpServletRequest.getHeader(RGPD_HEADER);

            MDC.put(AppConstants.MDC_CORRELATION_ID, correlationId);
            if (authHeader != null) {
                MDC.put(AppConstants.MDC_AUTHENTICATION, maskAuth(authHeader));
            }
            MDC.put(AppConstants.MDC_RGPD, rgpd != null ? rgpd : "false");

            // Default event metadata for web requests
            MDC.put(AppConstants.MDC_EVENT_CATEGORY, "web");
            MDC.put(AppConstants.MDC_EVENT_TYPE, "access");
            MDC.put(AppConstants.MDC_EVENT_OUTCOME, "success");
            MDC.put(AppConstants.MDC_HTTP_METHOD, httpServletRequest.getMethod());
            MDC.put(AppConstants.MDC_URL_PATH, httpServletRequest.getRequestURI());
        }

        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(AppConstants.MDC_CORRELATION_ID);
            MDC.remove(AppConstants.MDC_AUTHENTICATION);
            MDC.remove(AppConstants.MDC_RGPD);
            MDC.remove(AppConstants.MDC_EVENT_CATEGORY);
            MDC.remove(AppConstants.MDC_EVENT_TYPE);
            MDC.remove(AppConstants.MDC_EVENT_OUTCOME);
            MDC.remove(AppConstants.MDC_HTTP_METHOD);
            MDC.remove(AppConstants.MDC_URL_PATH);
        }
    }

    private String maskAuth(String auth) {
        if (auth == null || auth.length() < 15) {
            return "****";
        }
        return auth.substring(0, 12) + "...";
    }
}
