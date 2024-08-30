package org.example.consumer.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class WikimediaListener {

    @KafkaListener(topics = "wikimedia-topic", groupId = "wikimedia-event-consumers")
    public void process(String text) {
        log.info("Received: {}", text);
    }
}
