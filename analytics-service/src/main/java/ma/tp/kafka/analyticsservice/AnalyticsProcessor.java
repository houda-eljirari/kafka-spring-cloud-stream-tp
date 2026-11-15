
package ma.tp.kafka.analyticsservice;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class AnalyticsProcessor {

    private final ObjectMapper objectMapper;

    public AnalyticsProcessor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    private String extractType(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            JsonNode type = node.get("type");

            if (type != null && !type.isNull()) {
                return type.asText();
            }
        } catch (Exception e) {
            System.err.println("Erreur de lecture JSON : " + e.getMessage());
        }

        return "UNKNOWN";
    }

    @Bean
    public KStream<String, String> processEvents(StreamsBuilder builder) {

        KStream<String, String> events = builder.stream(
                List.of("tp-events-rest", "tp-events-supplier"),
                Consumed.with(Serdes.String(), Serdes.String())
        );

        events
                .map((key, value) ->
                        KeyValue.pair(extractType(value), value)
                )
                .groupByKey(
                        Grouped.with(Serdes.String(), Serdes.String())
                )
                .count(
                        Materialized.as("event-counts-store")
                )
                .toStream()
                .mapValues(String::valueOf)
                .to(
                        "tp-event-counts",
                        Produced.with(Serdes.String(), Serdes.String())
                );

        return events;
    }
}
