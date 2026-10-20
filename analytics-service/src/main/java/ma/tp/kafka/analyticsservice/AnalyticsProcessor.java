package ma.tp.kafka.analyticsservice;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class AnalyticsProcessor {

    @Bean
    public KStream<String, String> processEvents(StreamsBuilder builder) {

        KStream<String, String> events = builder.stream(
                "tp-events-docker",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        events
                .mapValues(value -> {
                    System.out.println("Événement analysé : " + value);
                    return value;
                })
                .to(
                        "tp-events-processed",
                        Produced.with(Serdes.String(), Serdes.String())
                );

        return events;
    }
}
