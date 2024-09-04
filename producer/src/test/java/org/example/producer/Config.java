package org.example.producer;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

@TestConfiguration
public class Config {
    @Primary
    @Bean
    public Flux<ServerSentEvent<String>> mockEventStream() {
        return Flux.just(ServerSentEvent
                .<String>builder().id("id").data("data").build());
    }
}
