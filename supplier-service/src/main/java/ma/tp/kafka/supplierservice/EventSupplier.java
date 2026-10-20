package ma.tp.kafka.supplierservice;

import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventSupplier {

    private static final Logger logger =
            LoggerFactory.getLogger(EventSupplier.class);

    @Bean
    public Supplier<Map<String, Object>> eventSupplier() {
        return () -> {
            Map<String, Object> event = Map.of(
                    "id", UUID.randomUUID().toString(),
                    "type", "SUPPLIER_EVENT",
                    "message", "Evenement genere automatiquement"
            );

            logger.info("Événement généré : {}", event);
            return event;
        };
    }
}