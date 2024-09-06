package org.example.consumer.listener;

import com.wikimedia.RecentChange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class WikimediaListener {
    private final RestHighLevelClient restHighLevelClient;
    // Batch listener with manual offset commiting
    @KafkaListener(topics = "wikimedia-avro-topic", groupId = "wikimedia-event-consumers")
    public void processBatch(List<RecentChange> messages, Acknowledgment acknowledgment) throws IOException {
        log.info("Received: {} messages", messages.size());

        BulkRequest bulkRequest = new BulkRequest();
        messages.stream()
                .map(this::getIndexRequest)
                .forEach(bulkRequest::add);

        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        acknowledgment.acknowledge();
        log.info("Successfully saved: {} items", response.getItems().length);
    }

    private IndexRequest getIndexRequest(RecentChange message) {
        String id = message.getMeta().getId();
        return new IndexRequest("wikimedia")
                .source(message, XContentType.JSON)
                .id(id);
    }
}
