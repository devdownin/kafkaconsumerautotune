package com.vaut.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for persistence strategies (DB vs File).
 */
@Data
@Component
@ConfigurationProperties(prefix = "app.persistence")
public class PersistenceProperties {

    /** If true, saves events to disk instead of the database. */
    private boolean saveInFile = false;

    /** Path to the directory where events will be traced. Defaults to "trace". */
    private String tracePath = "trace";

    /** Format for the traced files: "json" or "xml". Defaults to "json". */
    private String format = "json";
}
