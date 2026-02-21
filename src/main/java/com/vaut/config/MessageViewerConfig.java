package com.vaut.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Configuration for the UI message viewer, defining how to display specific blocks of data from the JSON payload.
 */
@Configuration
@ConfigurationProperties(prefix = "app.message-viewer")
@Data
public class MessageViewerConfig {
    /** List of custom block configurations for the message viewer. */
    private List<BlockConfig> blocks;

    /**
     * Individual block configuration for the message viewer.
     */
    @Data
    public static class BlockConfig {
        /** The title of the block displayed in the UI. */
        private String title;
        /** The JsonPath expression used to extract data for this block from the message payload. */
        private String jsonPath;
    }
}
