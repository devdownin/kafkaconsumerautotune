package com.vaut.service;

import com.vaut.dto.dashboard.KafkaPropertyOptimization;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Service that tracks and maintains a history of Kafka property optimizations.
 * Provides a log of parameter changes made by the auto-tuning service.
 */
@Service
public class KafkaOptimizerService {

    private final List<KafkaPropertyOptimization> optimizations = new ArrayList<>();

    /**
     * Initializes the optimizer service with some initial (mock) data for demonstration.
     */
    public KafkaOptimizerService() {
        // Initial mock data
        optimizations.add(KafkaPropertyOptimization.builder()
                .propertyName("max.poll.records")
                .oldValue("100")
                .newValue("450")
                .reason("PID optimization: error=0.352, throughput=1240.25 msg/s")
                .explanation("Augmentation de la taille du lot pour optimiser le débit. Le système traite les messages plus vite que l'objectif fixé, nous augmentons donc la charge par lot pour réduire l'overhead des polls.")
                .timestamp(LocalDateTime.now().minusHours(2))
                .build());

        optimizations.add(KafkaPropertyOptimization.builder()
                .propertyName("fetch.min.bytes")
                .oldValue("10240")
                .newValue("65536")
                .reason("Optimizing fetch efficiency for 1240.25 msg/s")
                .explanation("Optimisation de l'efficacité réseau. En attendant d'avoir plus de données avant de répondre, on réduit le nombre d'allers-retours réseau inutiles.")
                .timestamp(LocalDateTime.now().minusMinutes(45))
                .build());

        optimizations.add(KafkaPropertyOptimization.builder()
                .propertyName("max.poll.interval.ms")
                .oldValue("300000")
                .newValue("360000")
                .reason("Extending poll interval safety for target batch duration of 1200ms")
                .explanation("Ajustement de la marge de sécurité. Pour éviter que Kafka ne considère le consumer comme mort pendant un traitement long, nous augmentons le délai maximum autorisé entre deux lectures.")
                .timestamp(LocalDateTime.now().minusMinutes(120))
                .build());

        optimizations.add(KafkaPropertyOptimization.builder()
                .propertyName("fetch.max.bytes")
                .oldValue("52428800")
                .newValue("62914560")
                .reason("Scaling fetch size for max.poll.records=450 and avgMsgSize=820.5")
                .explanation("Adaptation de la mémoire tampon. La taille maximale des données récupérées est augmentée pour s'aligner sur le nombre croissant de messages demandés par lot.")
                .timestamp(LocalDateTime.now().minusDays(1))
                .build());
    }

    /**
     * Retrieves the most recent optimization events.
     *
     * @return A list of the 10 most recent KafkaPropertyOptimization objects, sorted by timestamp descending.
     */
    public List<KafkaPropertyOptimization> getRecentOptimizations() {
        List<KafkaPropertyOptimization> sorted = new ArrayList<>(optimizations);
        sorted.sort((a, b) -> b.timestamp().compareTo(a.timestamp()));
        return sorted.stream().limit(10).toList();
    }

    /**
     * Records a new optimization event.
     *
     * @param property The Kafka property that was changed.
     * @param oldVal The previous value of the property.
     * @param newVal The new value applied to the property.
     * @param reason The reason for the change.
     * @param explanation A pedagogical explanation of the change.
     */
    public void addOptimization(String property, String oldVal, String newVal, String reason, String explanation) {
        optimizations.add(0, KafkaPropertyOptimization.builder()
                .propertyName(property)
                .oldValue(oldVal)
                .newValue(newVal)
                .reason(reason)
                .explanation(explanation)
                .timestamp(LocalDateTime.now())
                .build());

        if (optimizations.size() > 50) {
            optimizations.remove(optimizations.size() - 1);
        }
    }
}
