package com.vaut;

import com.vaut.ConsotopicApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = ConsotopicApplication.class)
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9093", "port=9093" })
@ActiveProfiles("dev")
class ConsotopicApplicationTests {

	@Test
	void contextLoads() {
	}

}
