package org.example.producer;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.wikimedia.Meta;
import com.wikimedia.RecentChange;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import org.awaitility.Awaitility;
import org.example.producer.config.KafkaProducerConfig;
import org.example.producer.producer.WikimediaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = {KafkaProducerConfig.class, Config.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureWireMock(port = 0)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, topics = {"wikimedia-avro-topic"} )
class WikimediaProducerIT {
    public static final String WIKIMEDIA_AVRO_TOPIC = "wikimedia-avro-topic";
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private KafkaTestListener testReceiver;
    @Autowired
    private WikimediaProducer producer;

    @Configuration
    static class TestConfig {
        @Bean
        public KafkaTestListener testReceiver() {
            return new KafkaTestListener();
        }
    }

    public static class KafkaTestListener {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicReference<RecentChange> result = new AtomicReference<>();

        @KafkaListener(groupId = "PaymentIntegrationTest",containerFactory = "testKafkaListenerContainerFactory",
                topics = WIKIMEDIA_AVRO_TOPIC, autoStartup = "true")
        void receive(@Payload final RecentChange recentChange) {
            counter.incrementAndGet();
            result.set(recentChange);
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
        testReceiver.counter.set(0);
        WireMock.reset();
        WireMock.resetAllRequests();
        WireMock.resetAllScenarios();
        WireMock.resetToDefault();

        registerSchema(1, RecentChange.getClassSchema().toString());
    }

    private void registerSchema(int schemaId, String schema) throws IOException {
        String urlPath = "/subjects/" + WikimediaProducerIT.WIKIMEDIA_AVRO_TOPIC + "-value";
        stubFor(post(urlPathMatching(urlPath))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":" + schemaId + "}")));

        final SchemaString schemaString = new SchemaString(schema);
        stubFor(get(urlPathMatching("/schemas/ids/"+schemaId))
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json")
                        .withBody(schemaString.toJson())));
    }

    @Test
    public void testSend() {
        Meta meta = new Meta("https://example.com", "req123", "id456", "2024-09-04T17:29:13Z", "example.com", "stream1", "topic1", 1, 100L);
        RecentChange recentChange = new RecentChange(meta, 123456L, "edit", 0, "Dummy Title", "https://example.com/Dummy_Title", "This is a dummy comment.", 1693840153000L, // Unix timestamp for 2024-09-04T17:29:13Z
                "DummyUser", false, "https://example.com", "example.com", "/w", "dummywiki");

        String json = recentChange.toString();

        producer.sendEvent(ServerSentEvent.<String>builder().id("id").data(json).build());


        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(1000, TimeUnit.MILLISECONDS)
                .until(testReceiver.result::get, c->c.equals(recentChange));
    }
}
