package org.example.consumer;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.wikimedia.Meta;
import com.wikimedia.RecentChange;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.consumer.config.KafkaConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = {KafkaConsumerConfig.class, Config.class})
@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureWireMock(port = 0)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, topics = {"wikimedia-avro-topic"})
public class WikimediaConsumerIT {
    public static final String WIKIMEDIA_AVRO_TOPIC = "wikimedia-avro-topic";
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private KafkaTemplate<String, RecentChange> testKafkaTemplate;
    @MockBean(name = "restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

    @BeforeEach
    public void setUp() throws Exception {
        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
        WireMock.reset();
        WireMock.resetAllRequests();
        WireMock.resetAllScenarios();
        WireMock.resetToDefault();

        registerSchema(1, RecentChange.getClassSchema().toString());
    }

    private void registerSchema(int schemaId, String schema) throws IOException {
        String urlPath = "/subjects/" + WIKIMEDIA_AVRO_TOPIC + "-value";
        stubFor(post(urlPathMatching(urlPath))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":" + schemaId + "}")));

        final SchemaString schemaString = new SchemaString(schema);
        stubFor(get(urlPathMatching("/schemas/ids/" + schemaId))
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json")
                        .withBody(schemaString.toJson())));
    }

    @Test
    public void testConsume() throws ExecutionException, InterruptedException, IOException {
        Meta meta = new Meta("id456");
        RecentChange recentChange = new RecentChange(meta, 123456L, "comment", 1693840153000L, "DummyUser", "dummywiki");
        BulkResponse bulkResponse = new BulkResponse(new BulkItemResponse[1], 10, 20);

        final ProducerRecord<String, RecentChange> record =
                new ProducerRecord<>(WIKIMEDIA_AVRO_TOPIC, null, recentChange.getMeta().getId(), recentChange);

        given(restHighLevelClient.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).willReturn(bulkResponse);

        testKafkaTemplate.send(record).get();
        Thread.sleep(100);

        verify(restHighLevelClient).bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT));
    }
}
