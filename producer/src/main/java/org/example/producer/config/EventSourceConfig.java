package org.example.producer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Configuration
public class EventSourceConfig {

    public static final String DATA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    @Bean
    public WebClient webClient() {
        return WebClient.create(DATA_STREAM_URL);
    }

    @Bean
    public Flux<ServerSentEvent<String>> flux(WebClient client) {
        ParameterizedTypeReference<ServerSentEvent<String>> type = new ParameterizedTypeReference<>() {};
        return client.get()
                .retrieve()
                .bodyToFlux(type);
    }
}
