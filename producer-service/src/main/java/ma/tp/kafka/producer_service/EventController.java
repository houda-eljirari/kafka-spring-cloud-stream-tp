
package ma.tp.kafka.producer_service;

import java.util.Map;
import java.util.function.Supplier;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/events")
public class EventController {

    private final StreamBridge streamBridge;

    public EventController(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> sendEvent(@RequestBody Map<String, Object> event) {
        boolean sent = streamBridge.send("eventProducer-out-0", event);

        if (!sent) {
            throw new IllegalStateException("Impossible d'envoyer l'événement à Kafka");
        }

        return Map.of(
                "status", "sent",
                "topic", "tp-events-docker",
                "event", event
        );
    }
}
