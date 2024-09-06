package org.example.consumer.config;

import org.apache.http.HttpHost;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class OpenSearchConfig {
    @Bean
    public RestHighLevelClient restHighLevelClient(@Value("${opensearch.hostname}") String hostname,
                                                   @Value("${opensearch.port}") Integer port,
                                                   @Value("${opensearch.scheme}") String scheme) throws IOException {
        RestHighLevelClient restHighLevelClient =
                new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, scheme)));

        createIndexIfNotExists(restHighLevelClient);
        return restHighLevelClient;
    }

    private void createIndexIfNotExists(RestHighLevelClient restHighLevelClient) throws IOException {
        if (!restHighLevelClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
            restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        }
    }
}
