package com.vaut;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main entry point for the Consotopic application.
 * This application is a high-performance Kafka consumer with auto-tuning capabilities,
 * a monitoring dashboard, and Dead Letter Topic (DLT) management.
 */
@EnableCaching
@EnableRetry
@EnableScheduling
@SpringBootApplication
public class ConsotopicApplication {

	/**
	 * Main method to launch the Spring Boot application.
	 *
	 * @param args Command line arguments.
	 */
	public static void main(String[] args) {
		SpringApplication.run(ConsotopicApplication.class, args);
	}

}
