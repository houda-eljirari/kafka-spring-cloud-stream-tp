package ma.tp.kafka.analyticsservice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private final Map<String, Long> counts = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = "tp-event-counts",
            groupId = "analytics-api"
    )
    public void receiveCount(
            org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {

        try {
            counts.put(record.key(), Long.parseLong(record.value()));
        } catch (Exception e) {
            System.err.println("Compteur invalide : " + record.value());
        }
    }

    @GetMapping
    public Map<String, Long> getAnalytics() {
        return Map.copyOf(counts);
    }
}
