package org.example.consumer.listener;

import com.wikimedia.RecentChange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.consumer.repository.RecentChangeRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class WikimediaListener {
    private final RecentChangeRepository recentChangeRepository;

    // Batch listener with manual offset commiting
    @KafkaListener(topics = "wikimedia-avro-topic", groupId = "wikimedia-event-consumers")
    public void processBatch(List<RecentChange> messages, Acknowledgment acknowledgment) throws IOException {
        log.info("Received: {} messages", messages.size());

        recentChangeRepository.save(messages);

        acknowledgment.acknowledge();
    }
}
