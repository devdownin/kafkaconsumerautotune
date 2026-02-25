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

    private static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    private static final String AUTH_HEADER = "Authorization";
    private static final String RGPD_HEADER = "X-RGPD";

    private static final String MDC_CORRELATION_ID = "correlationId";
    private static final String MDC_AUTHENTICATION = "authentication";
    private static final String MDC_RGPD = "rgpd";
    private static final String MDC_EVENT_CATEGORY = "eventCategory";
    private static final String MDC_EVENT_TYPE = "eventType";
    private static final String MDC_EVENT_OUTCOME = "eventOutcome";

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest httpServletRequest) {
            String correlationId = httpServletRequest.getHeader(CORRELATION_ID_HEADER);
            if (correlationId == null || correlationId.isEmpty()) {
                correlationId = UUID.randomUUID().toString();
            }
            String authHeader = httpServletRequest.getHeader(AUTH_HEADER);
            String rgpd = httpServletRequest.getHeader(RGPD_HEADER);

            MDC.put(MDC_CORRELATION_ID, correlationId);
            if (authHeader != null) {
                MDC.put(MDC_AUTHENTICATION, maskAuth(authHeader));
            }
            MDC.put(MDC_RGPD, rgpd != null ? rgpd : "false");

            // Default event metadata for web requests
            MDC.put(MDC_EVENT_CATEGORY, "web");
            MDC.put(MDC_EVENT_TYPE, "access");
            MDC.put(MDC_EVENT_OUTCOME, "success");
        }

        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_CORRELATION_ID);
            MDC.remove(MDC_AUTHENTICATION);
            MDC.remove(MDC_RGPD);
            MDC.remove(MDC_EVENT_CATEGORY);
            MDC.remove(MDC_EVENT_TYPE);
            MDC.remove(MDC_EVENT_OUTCOME);
        }
    }

    private String maskAuth(String auth) {
        if (auth == null || auth.length() < 15) {
            return "****";
        }
        return auth.substring(0, 12) + "...";
    }
}
