package com.vaut.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Configuration for the application cache using Caffeine.
 */
@Configuration
public class CacheConfig {

    /**
     * Configures the application caches.
     *
     * <p>'stats' holds the assembled dashboard payload and turns over quickly so the UI stays live.
     * 'eventCounts' holds the aggregate row counts behind it on a longer schedule: those come from
     * full table scans and matter far less when slightly stale. Sharing the 5-second expiry would
     * mean re-running them on every scheduler tick, since the tick interval and the expiry are the
     * same.</p>
     *
     * @return The configured CacheManager.
     */
    @Bean
    public CacheManager cacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        cacheManager.setCaffeine(Caffeine.newBuilder()
                .expireAfterWrite(5, TimeUnit.SECONDS)
                .maximumSize(100));
        cacheManager.registerCustomCache("eventCounts", Caffeine.newBuilder()
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .maximumSize(10)
                .build());
        return cacheManager;
    }
}
