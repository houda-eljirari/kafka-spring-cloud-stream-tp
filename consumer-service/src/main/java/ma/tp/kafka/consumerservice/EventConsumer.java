
package ma.tp.kafka.consumerservice;

import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventConsumer {

    private static final Logger logger =
            LoggerFactory.getLogger(EventConsumer.class);

    @Bean
    public Consumer<Map<String, Object>> consumeEvent() {
        return event -> {
            logger.info("========== ÉVÉNEMENT KAFKA REÇU ==========");
            logger.info("Contenu : {}", event);
            logger.info("==========================================");
        };
    }
}
