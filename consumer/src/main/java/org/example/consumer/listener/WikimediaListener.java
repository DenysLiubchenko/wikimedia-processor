package org.example.consumer.listener;

import com.google.gson.JsonParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Slf4j
@RequiredArgsConstructor
public class WikimediaListener {
    private final RestHighLevelClient restHighLevelClient;

    @KafkaListener(topics = "wikimedia-topic", groupId = "wikimedia-event-consumers")
    public void process(ConsumerRecord<String, String> message) throws IOException {
        log.info("Received: {}", message);

        String id = extractId(message.value());
        IndexRequest indexRequest = new IndexRequest("wikimedia")
                .source(message.value(), XContentType.JSON)
                .id(id);
        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

        log.info("Successfully saved with id: {}", response.getId());
    }

    private String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject().get("meta")
                .getAsJsonObject().get("id")
                .getAsString();
    }
}
