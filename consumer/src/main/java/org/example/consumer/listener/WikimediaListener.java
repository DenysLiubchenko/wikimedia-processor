package org.example.consumer.listener;

import com.google.gson.JsonParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class WikimediaListener {
    private final RestHighLevelClient restHighLevelClient;

//    @KafkaListener(topics = "wikimedia-topic", groupId = "wikimedia-event-consumers")
    public void process(String message) throws IOException {
        log.info("Received: {}", message);

        IndexRequest indexRequest = getIndexRequest(message);
        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

        log.info("Successfully saved with id: {}", response.getId());
    }

    // Batch listener with manual offset commiting
    @KafkaListener(topics = "wikimedia-topic", groupId = "wikimedia-event-consumers")
    public void processBatch(List<String> messages) throws IOException {
        log.info("Received: {} messages", messages.size());

        BulkRequest bulkRequest = new BulkRequest();
        messages.stream()
                .map(this::getIndexRequest)
                .forEach(bulkRequest::add);

        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        log.info("Successfully saved: {} items", response.getItems().length);
    }

    private IndexRequest getIndexRequest(String message) {
        String id = extractId(message);
        return new IndexRequest("wikimedia")
                .source(message, XContentType.JSON)
                .id(id);
    }

    private String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject().get("meta")
                .getAsJsonObject().get("id")
                .getAsString();
    }
}
