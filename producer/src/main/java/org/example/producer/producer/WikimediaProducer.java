package org.example.producer.producer;

import com.launchdarkly.eventsource.EventSource;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class WikimediaProducer {
    private final EventSource eventSource;

    @PostConstruct
    public void startReadingStream() {
        eventSource.start();
    }

    @PreDestroy
    public void stopReadingStream() {
        eventSource.close();
    }
}
