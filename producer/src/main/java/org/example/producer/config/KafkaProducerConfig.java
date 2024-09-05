package org.example.producer.config;

import com.wikimedia.RecentChange;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ComponentScan("org.example.producer")
public class KafkaProducerConfig {
    @Bean
    public ProducerFactory<String, RecentChange> producerFactory(@Value("${kafka.schema.registry.url}") String schemaRegistry,
                                                                 @Value("${kafka.bootstrap-servers}") String bootstrapServer) {
        return new DefaultKafkaProducerFactory<>(producerConfig(schemaRegistry, bootstrapServer));
    }

    @Bean
    public KafkaTemplate<String, RecentChange> kafkaTemplate(ProducerFactory<String, RecentChange> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    private Map<String, Object> producerConfig(String schemaRegistry, String bootstrapServer) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // To compress messages using "snappy"
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // Set waiting for batches to fill up for 10 ms
        config.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        // Set batch size to 32Kb
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);

        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        config.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        return config;
    }
}
