package org.example.producer.producer;

import com.google.gson.Gson;
import com.wikimedia.RecentChange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class WikimediaProducer {
    private final KafkaTemplate<String, RecentChange> kafkaTemplate;
    private final Gson gson;

    public void sendEvent(ServerSentEvent<String> message) {
        if (message.data() == null) return;
        log.info("Processing event: {}", message);
        String data = message.data();
        RecentChange change = gson.fromJson(data, RecentChange.class);
        kafkaTemplate.send("wikimedia-avro-topic", change);
    }
}
