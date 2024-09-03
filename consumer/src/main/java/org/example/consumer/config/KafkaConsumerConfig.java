package org.example.consumer.config;

import com.wikimedia.RecentChange;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    @Value("${schema.registry.url}")
    public String schemaRegistry;
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Bean(name = "consumerFactoryV2")
    public ConsumerFactory<String, RecentChange> consumerFactoryV2() {
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    @Bean(name = "kafkaListenerContainerFactoryV2")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, RecentChange>>
    kafkaListenerContainerFactoryV2(
        ConsumerFactory<String, RecentChange> consumerFactory) {
        var factory =
            new ConcurrentKafkaListenerContainerFactory<String, RecentChange>();
        factory.setConsumerFactory(consumerFactory);
        // Enabling manual offset commit
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // Enable batch listening
        factory.setBatchListener(true);
        return factory;
    }

    private Map<String, Object> consumerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        // Disable Offset auto-commit
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        return config;
    }
}
