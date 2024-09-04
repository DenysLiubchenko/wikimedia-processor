package org.example.producer.eventListener;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.producer.producer.WikimediaProducer;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Component
@Slf4j
@RequiredArgsConstructor
public class WikimediaEventListener {
    private final WikimediaProducer producer;
    private final Flux<ServerSentEvent<String>> eventStream;
    @PostConstruct
    public void consumeServerSentEvent() {
        eventStream.subscribe(
                producer::sendEvent,
                error -> log.error("Error receiving: {}", error),
                () -> log.info("Completed"));
    }
}
