package org.example.producer.eventHandler;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class WikimediaEventHandler implements EventHandler {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void onOpen() {

    }

    @Override
    public void onClosed() {

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        log.info("Processing event: {}, with comment {}", messageEvent, s);
        kafkaTemplate.send("wikimedia-topic", messageEvent.getData());
    }

    @Override
    public void onComment(String s) {

    }

    @Override
    public void onError(Throwable throwable) {
        throw new RuntimeException(throwable.getMessage());
    }
}
