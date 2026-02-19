// Points clés à retenir
//
//Aspect				Implémentation
//Signature méthode		List<ConsumerRecord<K,V>> + Acknowledgment
//Commit				Manuel via acknowledgment.acknowledge() après succès
//Performance			Traiter en bloc (ex: saveAll() plutôt que boucle de save())
//Erreur				Exception remonte → retry automatique via ErrorHandler
//Parsing				Filter les messages invalides plutôt que fail tout le batch
//Métriques				Toujours mesurer throughput et latence
//
//Ce pattern garantit performance (batch DB), fiabilité (commit manuel + retry), et observabilité (metrics + logs structurés).

package com.vaut;



import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableRetry
@EnableScheduling
@SpringBootApplication() //exclude = { Neo4jDataAutoConfiguration.class, R2dbcAutoConfiguration.class })
public class ConsotopicApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsotopicApplication.class, args);
	}

}
