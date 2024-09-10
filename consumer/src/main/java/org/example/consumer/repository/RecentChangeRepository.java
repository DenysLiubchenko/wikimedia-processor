package org.example.consumer.repository;

import com.wikimedia.RecentChange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor
public class RecentChangeRepository {
    private final RestHighLevelClient restHighLevelClient;

    public void save(List<RecentChange> recentChanges) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        recentChanges.stream()
                .map(this::getIndexRequest)
                .forEach(bulkRequest::add);

        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info("Successfully saved: {} items", response.getItems().length);
    }

    private IndexRequest getIndexRequest(RecentChange message) {
        String id = message.getMeta().getId();
        return new IndexRequest("wikimedia")
                .source(message, XContentType.JSON)
                .id(id);
    }
}
