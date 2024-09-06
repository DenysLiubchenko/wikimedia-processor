package org.example.producer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Configuration
public class EventSourceConfig {
    @Bean
    public WebClient webClient(@Value("wikimedia.event.stream.url") String url) {
        return WebClient.create(url);
    }

    @Bean
    public Flux<ServerSentEvent<String>> flux(WebClient client) {
        ParameterizedTypeReference<ServerSentEvent<String>> type = new ParameterizedTypeReference<>() {};
        return client.get()
                .retrieve()
                .bodyToFlux(type);
    }
}
