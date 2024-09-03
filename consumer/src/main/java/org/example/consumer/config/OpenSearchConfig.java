package org.example.consumer.config;

import org.apache.http.HttpHost;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class OpenSearchConfig {
    @Bean
    public RestHighLevelClient restHighLevelClient() throws IOException {
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
        createIndexIfNotExists(restHighLevelClient);
        return restHighLevelClient;
    }

    private void createIndexIfNotExists(RestHighLevelClient restHighLevelClient) throws IOException {
        if (!restHighLevelClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
            restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        }
    }

    @Bean(name = "restHighLevelClientV2")
    public RestHighLevelClient restHighLevelClientV2() throws IOException {
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
        createIndexIfNotExistsV2(restHighLevelClient);
        return restHighLevelClient;
    }

    private void createIndexIfNotExistsV2(RestHighLevelClient restHighLevelClient) throws IOException {
        if (!restHighLevelClient.indices().exists(new GetIndexRequest("wikimedia-v2"), RequestOptions.DEFAULT)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia-v2");
            restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        }
    }
}
