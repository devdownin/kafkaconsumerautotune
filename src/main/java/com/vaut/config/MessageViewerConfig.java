package com.vaut.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "app.message-viewer")
@Data
public class MessageViewerConfig {
    private List<BlockConfig> blocks;

    @Data
    public static class BlockConfig {
        private String title;
        private String jsonPath;
    }
}
